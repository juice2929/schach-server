"""LOCAL development WebSocket adapter for verified tournament chess games.

Run explicitly with `python online_tournament_ws_game_dev.py` to bind ONLY
127.0.0.1:10002. It never imports, edits or launches production server.py.

Protocol additions to the existing dev lobby:
  tournament_match_state: {code, match_id} -> private board state
  tournament_move:        {code, match_id, move: "e2e4"}
  tournament_resign:      {code, match_id}
  tournament_claim_draw:  {code, match_id}

Every game mutation runs under the same asyncio.Lock as registrations and
turnier starts. Player identity comes solely from WebSocket membership.
This is NOT production ready: no storage, reconnects, clocks, match invitations,
or public deployment integration. Do not expose this port on the Internet.
"""

import asyncio
import json
import re

import websockets

from online_tournament_game_dev import VerifiedTournamentGames
from online_tournament_ws_dev import TournamentLobbyService, normalize_code


GAME_ACTIONS = frozenset({
    'tournament_match_state', 'tournament_move',
    'tournament_resign', 'tournament_claim_draw',
})
MATCH_ID_PATTERN = re.compile(r'R[1-3]M[1-4]\Z')


class VerifiedTournamentWebSocketService(TournamentLobbyService):
    """Adds verified games to the existing, unchanged development lobby."""

    def __init__(self):
        super().__init__()
        self.verified_games = {}  # tournament code -> VerifiedTournamentGames

    async def process(self, ws, data):
        if not isinstance(data, dict) or data.get('action') not in GAME_ACTIONS:
            return await super().process(ws, data)

        action = data['action']
        code = normalize_code(data.get('code'))
        match_id = data.get('match_id')
        if not isinstance(match_id, str) or not MATCH_ID_PATTERN.fullmatch(match_id):
            raise ValueError('Ungültige Turnierpartie.')
        # Ignore no client-supplied player identity or winner; reject it outright.
        if 'player_id' in data or 'winner' in data or 'winner_id' in data:
            raise ValueError('Spielerkennung und Sieger dürfen nicht vom Client vorgegeben werden.')

        async with self.lock:
            member = self.membership.get(ws)
            if member is None or member[0] != code:
                raise PermissionError('Du bist nicht Mitglied dieses Turniers.')
            _, actor_id = member  # server-issued identity tied to this socket
            room = self.rooms.get(code)
            if room is None or not room.started:
                raise ValueError('Turnier ist noch nicht gestartet.')
            games = self.verified_games.setdefault(code, VerifiedTournamentGames(room))
            if action == 'tournament_match_state':
                result = {'action': 'tournament_match_state',
                          'state': games.state(match_id, actor_id)}
                targets = [ws]
                board_changed = False
            else:
                if action == 'tournament_move':
                    outcome = games.move(match_id, actor_id, data.get('move'))
                elif action == 'tournament_resign':
                    outcome = games.resign(match_id, actor_id)
                else:
                    outcome = games.claim_draw(match_id, actor_id)
                match = room._get_match(match_id)
                targets = [self.members[code][pid] for pid in (match.white_id, match.black_id)
                           if pid in self.members[code]]
                result = {'action': 'tournament_game_update', **outcome}
                board_changed = outcome['event'] != 'move_accepted'
                # Capture immutable view while holding lock; do not send private game
                # moves to unrelated players in this room.
                bracket_payload = {'action': 'tournament_state',
                                   'snapshot': room.snapshot()} if board_changed else None
                bracket_targets = list(self.members[code].values()) if board_changed else []

        # Never keep the service lock during potentially slow socket sends.
        # Disconnections may make some destinations undeliverable; the verified
        # board and bracket remain authoritative in server memory.
        for target in targets:
            try:
                await self.send(target, result)
            except websockets.exceptions.ConnectionClosed:
                pass
        if board_changed:
            for target in bracket_targets:
                try:
                    await self.send(target, bracket_payload)
                except websockets.exceptions.ConnectionClosed:
                    pass

    async def disconnect(self, ws):
        # Leverage the existing lobby's disconnect semantics. Once started,
        # player seats stay reserved; rejoin/resume is deliberately NOT provided.
        member = self.membership.get(ws)
        await super().disconnect(ws)
        if member:
            code, _ = member
            if code not in self.rooms:
                self.verified_games.pop(code, None)


async def main():
    service = VerifiedTournamentWebSocketService()
    host, port = '127.0.0.1', 10002
    async with websockets.serve(service.handler, host, port, max_size=4096):
        print(f'Lokaler Testserver: ws://{host}:{port}')
        print('Nur Entwicklungszwecke. Produktiven Render-Server nicht geändert.')
        await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())
