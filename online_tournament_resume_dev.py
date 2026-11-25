"""Local-only reconnection adapter for verified WebSocket tournament games.

Launch explicitly with ``python online_tournament_resume_dev.py`` on
127.0.0.1:10003. Does not import, modify, or start production server.py.

Create/join adds one PRIVATE ``tournament_resume_credentials`` message:
    {"action":"tournament_resume_credentials", "code":"...", "token":"..."}
The earlier create/join acknowledgement still returns its server-issued player_id.

After a disconnect (once the tournament has started), reconnect with:
    {"action":"tournament_resume", "code":"...", "player_id":"...", "token":"..."}
The response ``tournament_resumed`` contains the public tournament snapshot
and the player's own currently playable game state(s), including move history.

A random 256-bit token serves as a temporary bearer credential; only its
SHA-256 digest is stored server-side. A player who is still connected cannot
be impersonated by a second socket. Token remains valid for this server's
lifetime, including across repeated disconnects, to avoid losing access when
an acknowledgement itself is lost. Treat tokens as passwords.

STILL NOT PRODUCTION READY: in-memory-only state/tokens, no persistent identity,
rate limits, timeout clocks, invitation notifications, or public deployments.
Do not expose the local development port to the Internet.
"""

import asyncio
import hashlib
import hmac
import json
import secrets
from time import monotonic

import websockets

from online_tournament_game_dev import VerifiedTournamentGames
from online_tournament_ws_dev import normalize_code
from online_tournament_ws_game_dev import VerifiedTournamentWebSocketService


class ResumableVerifiedTournamentWebSocketService(VerifiedTournamentWebSocketService):
    def __init__(self):
        super().__init__()
        self._lobby_resume_enabled = set()
        self._lobby_deadlines = {}
        self._replaced_socket_tasks = set()
        self.lobby_grace_seconds = 120
        self._resume_proofs = {}  # (code, server-issued player_id) -> SHA-256(token)

    @staticmethod
    def _proof(token):
        return hashlib.sha256(token.encode('ascii')).digest()

    async def process(self, ws, data):
        if not isinstance(data, dict):
            return await super().process(ws, data)
        action = data.get('action')
        if action == 'tournament_leave':
            return await self._leave_lobby(ws, data)
        if action == 'tournament_resume':
            return await self._resume(ws, data)
        if action not in ('tournament_create', 'tournament_join'):
            return await super().process(ws, data)

        # Preserve the existing, tested registration and broadcast protocol.
        await super().process(ws, data)
        async with self.lock:
            membership = self.membership.get(ws)
            if membership is None:
                return
            code, player_id = membership
            token = secrets.token_urlsafe(32)  # 256-bit unpredictable secret
            self._resume_proofs[(code, player_id)] = self._proof(token)
            if data.get('lobby_resume') is True:
                self._lobby_resume_enabled.add((code, player_id))
        # Sent only to the registering player's connection; never broadcast.
        try:
            await self.send(ws, {'action': 'tournament_resume_credentials',
                                 'code': code, 'token': token})
        except websockets.exceptions.ConnectionClosed:
            pass

    async def _close_replaced_socket(self, ws):
        try:
            await ws.close(code=1000, reason='Connection replaced')
        except (websockets.exceptions.ConnectionClosed, OSError):
            pass

    async def _resume(self, ws, data):
        code = normalize_code(data.get('code'))
        player_id = data.get('player_id')
        token = data.get('token')
        if not isinstance(player_id, str) or len(player_id) != 32 or not all(
                c in '0123456789abcdef' for c in player_id):
            raise ValueError('Ungültige Server-Spielerkennung.')
        if not isinstance(token, str) or len(token) != 43 or not all(
                c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_' for c in token):
            raise PermissionError('Wiederverbindungsdaten ungültig.')
        if 'winner' in data or 'winner_id' in data:
            raise ValueError('Sieger darf nicht vom Client vorgegeben werden.')
        async with self.lock:
            if ws in self.membership:
                raise ValueError('Diese Verbindung ist bereits einem Turnier zugeordnet.')
            room = self.rooms.get(code)
            if (room is None or player_id not in room.players
                    or (not room.started and
                        (code, player_id) not in self._lobby_resume_enabled)):
                raise PermissionError('Wiederverbindung nicht zulässig.')
            if (not room.started and
                    self._lobby_deadlines.get((code, player_id), float('inf')) <= monotonic()):
                raise PermissionError('Lobby reservation expired.')
            expected = self._resume_proofs.get((code, player_id))
            proof = self._proof(token)
            if expected is None or not hmac.compare_digest(expected, proof):
                raise PermissionError('Wiederverbindung nicht zulässig.')
            replaced_ws = self.members[code].get(player_id)

            if replaced_ws is not None:
                if (room.started
                        or (code, player_id) not in self._lobby_resume_enabled
                        or data.get('replace_connection') is not True):
                    raise PermissionError('Spieler ist bereits verbunden.')

                self.membership.pop(replaced_ws, None)

            self._lobby_deadlines.pop((code, player_id), None)
            self.members[code][player_id] = ws
            self.membership[ws] = (code, player_id)

            if replaced_ws is not None:
                task = asyncio.create_task(
                    self._close_replaced_socket(replaced_ws)
                )
                self._replaced_socket_tasks.add(task)
                task.add_done_callback(
                    self._replaced_socket_tasks.discard
                )
            # A recovering player only receives details for their own ready
            # games; all other players' private board states remain private.
            game_states = []
            if room.started:
                games = self.verified_games.setdefault(code, VerifiedTournamentGames(room))
                for match in room.ready_matches():
                    if player_id in (match.white_id, match.black_id):
                        game_states.append(games.state(match.match_id, player_id))
            reply = {'action': 'tournament_resumed', 'code': code,
                     'snapshot': room.snapshot(), 'game_states': game_states}
        try:
            await self.send(ws, reply)
        except websockets.exceptions.ConnectionClosed:
            pass
        # Inform other participants that the seat has returned, without any
        # secret or private match state in the broadcast.
        await self.broadcast(code, 'tournament_player_reconnected')


    def _forget_lobby(self, code):
        self.verified_games.pop(code, None)

        for key in list(self._resume_proofs):
            if key[0] == code:
                self._resume_proofs.pop(key, None)
                self._lobby_resume_enabled.discard(key)
                self._lobby_deadlines.pop(key, None)

    def _remove_waiting_player_locked(self, code, player_id):
        room = self.rooms.get(code)

        if room is None or room.started or player_id not in room.players:
            return None

        ws = self.members[code].pop(player_id, None)
        if ws is not None:
            self.membership.pop(ws, None)

        key = (code, player_id)
        self._resume_proofs.pop(key, None)
        self._lobby_resume_enabled.discard(key)
        self._lobby_deadlines.pop(key, None)

        if player_id == room.host_id:
            peers = list(self.members[code].values())

            for peer in peers:
                self.membership.pop(peer, None)

            del self.rooms[code]
            del self.members[code]

            self._forget_lobby(code)
            return code, True, peers

        room.players.pop(player_id, None)
        return code, False, []

    async def _notify_lobby_removal(self, notice, reason):
        if notice is None:
            return

        code, cancelled, peers = notice

        if cancelled:
            for peer in peers:
                try:
                    await self.send(peer, {
                        'action': 'tournament_cancelled',
                        'reason': reason
                    })
                except websockets.exceptions.ConnectionClosed:
                    pass
        else:
            await self.broadcast(code)

    async def _leave_lobby(self, ws, data):
        code = normalize_code(data.get('code'))

        async with self.lock:
            member = self.membership.get(ws)

            if member is None or member[0] != code:
                raise PermissionError('Not a tournament member.')

            room = self.rooms.get(code)

            if room is None or room.started:
                raise ValueError('Lobby leave only before tournament start.')

            notice = self._remove_waiting_player_locked(code, member[1])

        try:
            await self.send(ws, {
                'action': 'tournament_left',
                'code': code
            })
        except websockets.exceptions.ConnectionClosed:
            pass

        await self._notify_lobby_removal(
            notice, 'Host hat die Lobby verlassen.'
        )

    async def _expire_lobby_reservations(self, now=None):
        if now is None:
            now = monotonic()

        notices = []

        async with self.lock:
            for key, deadline in list(self._lobby_deadlines.items()):
                if deadline > now or key not in self._lobby_deadlines:
                    continue

                self._lobby_deadlines.pop(key, None)

                notice = self._remove_waiting_player_locked(*key)

                if notice is not None:
                    notices.append(notice)

        for notice in notices:
            await self._notify_lobby_removal(
                notice, 'Wiederverbindungsfrist abgelaufen.'
            )

    async def expire_lobby_reservations(self, now=None):
        await self._expire_lobby_reservations(now)

    async def disconnect(self, ws):
        prior = self.membership.get(ws)
        reserved = False
        async with self.lock:
            if prior is not None:
                code, player_id = prior
                room = self.rooms.get(code)
                if (room is not None and not room.started
                        and prior in self._lobby_resume_enabled
                        and prior in self._resume_proofs
                        and self.members.get(code, {}).get(player_id) is ws):
                    self.membership.pop(ws, None)
                    self.members[code].pop(player_id, None)
                    self._lobby_deadlines[prior] = (
                        monotonic() + self.lobby_grace_seconds
                    )
                    reserved = True
        if reserved:
            await self.broadcast(code, 'tournament_player_disconnected')
            return
        await super().disconnect(ws)
        if prior is None:
            return
        code, player_id = prior
        async with self.lock:
            room = self.rooms.get(code)
            if room is None:
                # Includes host cancellation of an unstarted lobby.
                for key in [key for key in self._resume_proofs if key[0] == code]:
                    self._resume_proofs.pop(key, None)
                    self._lobby_resume_enabled.discard(key)
                    self._lobby_deadlines.pop(key, None)
            elif not room.started:
                # Before start, the old lobby correctly removes the player.
                self._resume_proofs.pop((code, player_id), None)
                self._lobby_resume_enabled.discard((code, player_id))
                self._lobby_deadlines.pop((code, player_id), None)


async def main():
    service = ResumableVerifiedTournamentWebSocketService()
    host, port = '127.0.0.1', 10003
    async with websockets.serve(service.handler, host, port, max_size=4096):
        print(f'Lokaler Wiederverbindungs-Testserver: ws://{host}:{port}')
        print('Nur Entwicklungszwecke. Produktiven Render-Server nicht geändert.')
        await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())
