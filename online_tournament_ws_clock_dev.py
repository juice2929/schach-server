"""Local-only timed WebSocket adapter, separate from the existing test server.
All operations run in one ordered dispatch gate; game mutations additionally
use the inherited service lock. Intended only for localhost development.
"""
import asyncio
from time import monotonic_ns
import websockets
from online_tournament_resume_dev import ResumableVerifiedTournamentWebSocketService
from online_tournament_timed_games_dev import TimedTournamentGames
from online_tournament_clock_dev import validate_minutes
from online_tournament_ws_dev import normalize_code


class TimedTournamentWebSocketService(ResumableVerifiedTournamentWebSocketService):
    def __init__(self, now=monotonic_ns):
        super().__init__()
        self.now = now
        self.minutes = {}
        self.dispatch = asyncio.Lock()

    async def process(self, ws, data):
        async with self.dispatch:
            if isinstance(data, dict) and data.get('action') == 'tournament_create':
                code = normalize_code(data.get('code'))
                minutes = validate_minutes(data.get('time_minutes', 0))
                if code not in self.rooms:
                    self.minutes[code] = minutes
                    try:
                        return await super().process(ws, data)
                    finally:
                        if code not in self.rooms:
                            self.minutes.pop(code, None)
            return await super().process(ws, data)

    async def disconnect(self, ws):
        async with self.dispatch:
            member = self.membership.get(ws)
            await super().disconnect(ws)
            if member and member[0] not in self.rooms:
                self.minutes.pop(member[0], None)

    async def send(self, ws, payload):
        snapshot = payload.get('snapshot')
        if isinstance(snapshot, dict):
            minutes = self.minutes.get(snapshot.get('code'), 0)
            if minutes:
                payload = {**payload, 'snapshot': {**snapshot, 'time_minutes': minutes}}
        await super().send(ws, payload)

    async def broadcast(self, code, action='tournament_state'):
        # The parent calls this after room.start but before notifying any client.
        # The dispatch gate prevents another request slipping into that gap.
        if action == 'tournament_started' and self.minutes.get(code, 0):
            async with self.lock:
                games = TimedTournamentGames(self.rooms[code], self.minutes[code], self.now)
                games.start_clocks()
                self.verified_games[code] = games
        await super().broadcast(code, action)

    async def _deliver(self, targets, payload):
        for target in targets:
            try:
                await self.send(target, payload)
            except websockets.exceptions.ConnectionClosed:
                pass

    async def poll_once(self, publish_clocks=False):
        """Resolve flags, then publish private clock samples and public brackets.

        Clock samples never carry a board or change match state on the client.
        draw_replays distinguishes successive games of the same pairing.
        """
        async with self.dispatch:
            packets = []
            async with self.lock:
                for code, games in list(self.verified_games.items()):
                    if not isinstance(games, TimedTournamentGames):
                        continue
                    room = self.rooms.get(code)
                    if room is None:
                        continue
                    members = self.members[code]
                    outcomes = games.poll_expired()
                    for outcome in outcomes:
                        match = room._get_match(outcome['state']['match_id'])
                        targets = [members[p] for p in (match.white_id, match.black_id) if p in members]
                        packets.append((targets, {'action': 'tournament_game_update', **outcome}))
                    if outcomes:
                        packets.append((list(members.values()), {'action': 'tournament_state',
                                                                 'snapshot': room.snapshot()}))
                    if publish_clocks:
                        now = self.now()
                        for match in room.ready_matches():
                            clock = games.clocks.get(match.match_id)
                            targets = [members[p] for p in (match.white_id, match.black_id) if p in members]
                            packets.append((targets, {'action': 'tournament_clock', 'code': code,
                                                       'match_id': match.match_id,
                                                       'draw_replays': match.draw_replays,
                                                       'clock': clock.snapshot(now)}))
            for targets, payload in packets:
                await self._deliver(targets, payload)

    async def monitor(self, poll_interval=.1, publish_every=5):
        count = 0
        while True:
            await asyncio.sleep(poll_interval)
            count += 1
            await self.poll_once(publish_clocks=(count % publish_every == 0))


async def main():
    service = TimedTournamentWebSocketService()
    async with websockets.serve(service.handler, '127.0.0.1', 10003, max_size=4096):
        print('Lokaler Schachuhr-Testserver: ws://127.0.0.1:10003')
        print('Nur Entwicklung. Render unveraendert. Alte Clients erstellen ohne Zeit.')
        await service.monitor()


if __name__ == '__main__':
    asyncio.run(main())
