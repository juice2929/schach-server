"""Isolated V1.3 candidate: same wire protocol, bounded sends and clock pauses.
Not deployed. No changes to Windows or the normal multiplayer server.
"""
import asyncio
import hmac
import json
import logging
from time import monotonic_ns

import websockets
from online_tournament_resume_dev import ResumableVerifiedTournamentWebSocketService
from online_tournament_ws_clock_dev import TimedTournamentWebSocketService
from online_tournament_timed_games_dev import TimedTournamentGames
from online_tournament_clock_dev import validate_minutes
from online_tournament_ws_dev import normalize_code

LOG = logging.getLogger('tournament.reliability')
PAUSE_NS = 120 * 1_000_000_000
SEND_TIMEOUT = 2
QUEUE_LIMIT = 64


class ReliableTournamentService(TimedTournamentWebSocketService):
    def __init__(self, now=monotonic_ns, pause_ns=PAUSE_NS):
        super().__init__(now)
        self.pause_ns = pause_ns
        self.budgets = {}  # (room, match) -> unused ns; shared across draw replays
        self.pauses = {}  # (room, match) -> (clock object, start_ns)
        self.outboxes = {}
        self.retired = set()  # superseded sockets; reject their queued actions

    @staticmethod
    def _abort(ws):
        transport = getattr(ws, 'transport', None)
        if transport is not None:
            transport.abort()

    async def _writer(self, ws, outbox):
        try:
            while True:
                payload = await outbox.get()
                await asyncio.wait_for(ws.send(payload), SEND_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except (OSError, TimeoutError, websockets.exceptions.WebSocketException):
            LOG.info('outgoing connection failed or stalled')
            self._abort(ws)

    async def handler(self, ws):
        outbox = asyncio.Queue(maxsize=QUEUE_LIMIT)
        self.outboxes[ws] = outbox
        writer = asyncio.create_task(self._writer(ws, outbox))
        try:
            await super().handler(ws)
        finally:
            self.outboxes.pop(ws, None)
            writer.cancel()
            await asyncio.gather(writer, return_exceptions=True)
            self.retired.discard(ws)

    async def send(self, ws, payload):
        # Snapshot at enqueue time, preserve per-socket ordering, never await I/O
        # while the state mutation/dispatch gate is held.
        outbox = self.outboxes.get(ws)
        if outbox is None:
            return
        if payload.get('action') == 'tournament_error':
            reason = {'Spieler ist bereits verbunden.': 'seat_busy',
                      'Wiederverbindung nicht zulässig.': 'resume_not_permitted',
                      'Wiederverbindungsdaten ungültig.': 'invalid_credentials',
                      'Ungültige Server-Spielerkennung.': 'invalid_player_id',
                      'Partie ist bereits beendet.': 'match_finished',
                      'Du bist nicht Mitglied dieses Turniers.': 'not_member',
                      'Du spielst nicht in dieser Partie.': 'not_participant',
                      'Du bist nicht am Zug.': 'not_turn'}.get(payload.get('message'), 'other')
            LOG.info('protocol error (%s); registered=%s', reason, ws in self.membership)
        snapshot = payload.get('snapshot')
        if isinstance(snapshot, dict):
            minutes = self.minutes.get(snapshot.get('code'), 0)
            if minutes:
                payload = {**payload, 'snapshot': {**snapshot, 'time_minutes': minutes}}
        try:
            outbox.put_nowait(json.dumps(payload, ensure_ascii=False))
        except asyncio.QueueFull:
            LOG.info('outgoing queue full; closing slow connection')
            self._abort(ws)

    async def _publish_state(self, code, games, match, event, only=None):
        members = self.members[code]
        for pid in (match.white_id, match.black_id):
            if pid in members and (only is None or only == pid):
                await self.send(members[pid], {
                    'action': 'tournament_game_update', 'event': event,
                    'state': games.state(match.match_id, pid)})

    async def _sync_pauses(self):
        # All callers hold dispatch. A full state is necessary because the
        # released client ignores clock-only samples with active=None.
        now = self.now()
        ready = set()
        for code, games in list(self.verified_games.items()):
            if not isinstance(games, TimedTournamentGames) or code not in self.rooms:
                continue
            members = self.members[code]
            for match in self.rooms[code].ready_matches():
                key = (code, match.match_id)
                ready.add(key)
                clock = games.clocks.get(match.match_id)
                if clock is None:
                    continue
                remaining = self.budgets.get(key, self.pause_ns)
                offline = any(pid not in members for pid in (match.white_id, match.black_id))
                previous = self.pauses.get(key)
                if previous:
                    old_clock, start = previous
                    if old_clock is not clock:
                        # Defensive: a replay should not occur while paused.
                        remaining = max(0, remaining - (now - start))
                        self.budgets[key] = remaining
                        self.pauses.pop(key, None)
                    elif not offline or now - start >= remaining:
                        # Charge at most the budget; elapsed time after its end
                        # counts as normal playing time even if the poll is late.
                        end = min(now, start + remaining)
                        self.budgets[key] = max(0, remaining - (end - start))
                        clock.resume(end)
                        self.pauses.pop(key, None)
                        await self._publish_state(code, games, match, 'clock_resumed')
                        LOG.info('clock resumed (%s)', match.match_id)
                        continue
                    else:
                        continue
                if offline and remaining > 0 and clock.pause(now):
                    self.pauses[key] = (clock, now)
                    await self._publish_state(code, games, match, 'clock_paused')
                    LOG.info('clock paused (%s)', match.match_id)
        for mapping in (self.pauses, self.budgets):
            for key in list(mapping):
                if key not in ready:
                    mapping.pop(key, None)

    async def _resume(self, ws, data):
        # A valid private token authenticates a replacement connection. A
        # transport-level pong does not demonstrate that the old application
        # is still usable (e.g. a proxy can keep that transport alive).
        # process holds dispatch; validation precedes any mutation.
        code = normalize_code(data.get('code'))
        pid, token = data.get('player_id'), data.get('token')
        valid_format = (isinstance(pid, str) and len(pid) == 32 and
                        all(c in '0123456789abcdef' for c in pid) and
                        isinstance(token, str) and len(token) == 43 and
                        all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_' for c in token))
        if (valid_format and ws not in self.membership and
                'winner' not in data and 'winner_id' not in data):
            room = self.rooms.get(code)
            old = self.members.get(code, {}).get(pid)
            expected = self._resume_proofs.get((code, pid))
            if (room is not None and room.started and pid in room.players and
                    old is not None and old is not ws and expected is not None and
                    hmac.compare_digest(expected, self._proof(token))):
                # Revoke authority BEFORE closing the old transport. Its
                # queued actions and late disconnect cannot affect the seat.
                # Do not create an artificial offline interval or reset clocks.
                self.retired.add(old)
                self.members[code].pop(pid, None)
                self.membership.pop(old, None)
                self.outboxes.pop(old, None)
                self._abort(old)
                LOG.info('authenticated seat replaced')
        await super()._resume(ws, data)
        LOG.info('resume accepted')

    async def process(self, ws, data):
        async with self.dispatch:
            if ws in self.retired:
                return
            await self._expire_lobby_heartbeats()
            await self._expire_lobby_reservations()
            await self._sync_pauses()
            # Same creation path as the timed adapter; call its parent so
            # the dispatch lock is not acquired twice.
            if isinstance(data, dict):
                action = data.get('action')
                if action == 'tournament_create':
                    code = normalize_code(data.get('code'))
                    minutes = validate_minutes(data.get('time_minutes', 0))
                    if code not in self.rooms:
                        self.minutes[code] = minutes
                        try:
                            return await ResumableVerifiedTournamentWebSocketService.process(self, ws, data)
                        finally:
                            if code not in self.rooms:
                                self.minutes.pop(code, None)
                if action in ('tournament_move', 'tournament_claim_draw'):
                    code = normalize_code(data.get('code'))
                    member = self.membership.get(ws)
                    mid = data.get('match_id')
                    # Reject a move on hold without tournament_error: the
                    # released client treats even later errors as resume errors.
                    if (member and member[0] == code and isinstance(mid, str) and
                            (code, mid) in self.pauses and
                            not any(k in data for k in ('player_id', 'winner', 'winner_id'))):
                        games = self.verified_games[code]
                        match = games._authorized_match(mid, member[1])
                        await self._publish_state(code, games, match, 'clock_paused', only=member[1])
                        return
            try:
                result = await ResumableVerifiedTournamentWebSocketService.process(self, ws, data)
                if isinstance(data, dict) and data.get('action') == 'tournament_resign':
                    LOG.info('resign request processed')
                return result
            finally:
                await self._sync_pauses()

    async def disconnect(self, ws):
        async with self.dispatch:
            member = self.membership.get(ws)
            await ResumableVerifiedTournamentWebSocketService.disconnect(self, ws)
            if member:
                if member[0] not in self.rooms:
                    self.minutes.pop(member[0], None)
                LOG.info('participant disconnected')
            await self._sync_pauses()

    async def poll_once(self, publish_clocks=False):
        # Maintain the same packet order and clock protocol as the deployed
        # timed adapter. Pause/resume and flag checks share one dispatch gate.
        async with self.dispatch:
            await self._expire_lobby_heartbeats()
            await self._expire_lobby_reservations()
            await self._sync_pauses()
            packets = []
            async with self.lock:
                for code, games in list(self.verified_games.items()):
                    if not isinstance(games, TimedTournamentGames) or code not in self.rooms:
                        continue
                    room, members = self.rooms[code], self.members[code]
                    outcomes = games.poll_expired()
                    for outcome in outcomes:
                        match = room._get_match(outcome['state']['match_id'])
                        targets = [members[p] for p in (match.white_id, match.black_id) if p in members]
                        packets.append((targets, {'action': 'tournament_game_update', **outcome}))
                    if outcomes:
                        packets.append((list(members.values()), {'action': 'tournament_state', 'snapshot': room.snapshot()}))
                    if publish_clocks:
                        now = self.now()
                        for match in room.ready_matches():
                            clock = games.clocks.get(match.match_id)
                            targets = [members[p] for p in (match.white_id, match.black_id) if p in members]
                            packets.append((targets, {'action': 'tournament_clock', 'code': code,
                                'match_id': match.match_id, 'draw_replays': match.draw_replays,
                                'clock': clock.snapshot(now)}))
            for targets, payload in packets:
                await self._deliver(targets, payload)
            await self._sync_pauses()


async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(message)s')
    service = ReliableTournamentService()
    # Local-only entry point, no production deployment.
    async with websockets.serve(service.handler, '127.0.0.1', 10003,
                               max_size=4096, ping_interval=5, ping_timeout=5, close_timeout=2):
        print('Lokaler Wiederverbindungs- und Pausentest: ws://127.0.0.1:10003', flush=True)
        await service.monitor()


if __name__ == '__main__':
    asyncio.run(main())
