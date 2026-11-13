"""Local-only WebSocket tournament lobby prototype for Schachspiel Ultimate Online.

Does NOT modify or start the production server.py and does NOT handle chess moves.
All connection/player IDs are server-generated. No client-supplied game result is accepted.
No persistence, reconnection, or real-world authentication yet.
"""
import asyncio
import json
import re
import uuid
import websockets

from online_tournament_core import TournamentRoom


HOST = '127.0.0.1'
PORT = 10001
CODE_PATTERN = re.compile(r'[A-Z0-9-]{3,20}\Z')


def normalize_code(value):
    if not isinstance(value, str):
        raise ValueError('Turniercode muss Text sein.')
    code = value.strip().upper()
    if not CODE_PATTERN.fullmatch(code):
        raise ValueError('Turniercode: 3-20 Zeichen (A-Z, 0-9, -).')
    return code


class TournamentLobbyService:
    def __init__(self):
        self.rooms = {}            # code -> TournamentRoom
        self.members = {}          # code -> {server-generated player_id: websocket}
        self.membership = {}       # websocket -> (code, player_id)
        self.lock = asyncio.Lock()

    @staticmethod
    async def send(ws, payload):
        await ws.send(json.dumps(payload, ensure_ascii=False))

    async def broadcast(self, code, action='tournament_state'):
        # State changes happen under lock before this is called.
        room = self.rooms.get(code)
        if room is None:
            return
        payload = {'action': action, 'snapshot': room.snapshot()}
        for ws in list(self.members.get(code, {}).values()):
            try:
                await self.send(ws, payload)
            except websockets.exceptions.ConnectionClosed:
                pass

    async def process(self, ws, data):
        if not isinstance(data, dict):
            raise ValueError('JSON-Objekt erforderlich.')
        action = data.get('action')
        if action not in {'tournament_create', 'tournament_join', 'tournament_start', 'tournament_state'}:
            raise ValueError('Unbekannte oder nicht erlaubte Turnieraktion.')
        code = normalize_code(data.get('code'))
        notice = None
        snapshot = None
        player_id = None
        async with self.lock:
            if action in ('tournament_create', 'tournament_join'):
                if ws in self.membership:
                    raise ValueError('Diese Verbindung ist bereits in einem Turnier.')
                name = data.get('name')
                player_id = uuid.uuid4().hex  # ID comes from the connection, not JSON
                if action == 'tournament_create':
                    if code in self.rooms:
                        raise ValueError('Turniercode ist schon vergeben.')
                    capacity = data.get('capacity')
                    if type(capacity) is not int:
                        raise ValueError('Teilnehmerzahl muss 4 oder 8 sein.')
                    room = TournamentRoom(code, capacity, player_id, name)
                    self.rooms[code] = room
                    self.members[code] = {}
                    notice = 'tournament_created'
                else:
                    if code not in self.rooms:
                        raise ValueError('Turnierraum existiert nicht.')
                    room = self.rooms[code]
                    room.register(player_id, name)
                    notice = 'tournament_joined'
                self.members[code][player_id] = ws
                self.membership[ws] = (code, player_id)
            else:
                if self.membership.get(ws, (None,))[0] != code:
                    raise PermissionError('Du bist nicht Mitglied dieses Turniers.')
                room = self.rooms[code]
                _, player_id = self.membership[ws]
                if action == 'tournament_start':
                    # No phantom/disconnected participants may fill the lobby.
                    if len(self.members[code]) != room.capacity:
                        raise ValueError('Nicht alle Teilnehmer sind verbunden.')
                    room.start(player_id)
                    notice = 'tournament_started'
                else:
                    notice = 'tournament_state'
            snapshot = room.snapshot()
        if action in ('tournament_create', 'tournament_join'):
            await self.send(ws, {'action': notice, 'code': code, 'player_id': player_id, 'snapshot': snapshot})
            await self.broadcast(code)
        elif action == 'tournament_start':
            await self.broadcast(code, notice)
        else:
            await self.send(ws, {'action': notice, 'snapshot': snapshot})

    async def disconnect(self, ws):
        async with self.lock:
            member = self.membership.pop(ws, None)
            if member is None:
                return
            code, player_id = member
            room = self.rooms.get(code)
            if room is None:
                return
            self.members[code].pop(player_id, None)
            if not room.started:
                if player_id == room.host_id:
                    peers = list(self.members[code].values())
                    del self.rooms[code]
                    del self.members[code]
                    for peer in peers:
                        self.membership.pop(peer, None)
                    cancelled = True
                else:
                    room.players.pop(player_id, None)
                    cancelled = False
                    peers = []
            else:
                cancelled = False
                peers = []
        if cancelled:
            for peer in peers:
                try:
                    await self.send(peer, {'action':'tournament_cancelled', 'reason':'Host hat die Lobby verlassen.'})
                except websockets.exceptions.ConnectionClosed:
                    pass
        elif room.started:
            await self.broadcast(code, 'tournament_player_disconnected')
        else:
            await self.broadcast(code)

    async def handler(self, ws):
        try:
            async for raw in ws:
                try:
                    if not isinstance(raw, str) or len(raw) > 4096:
                        raise ValueError('Nachricht ungültig oder zu lang.')
                    data = json.loads(raw)
                    await self.process(ws, data)
                except (json.JSONDecodeError, ValueError, TypeError, PermissionError) as exc:
                    await self.send(ws, {'action':'tournament_error', 'message':str(exc)})
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            await self.disconnect(ws)


async def main():
    service = TournamentLobbyService()
    async with websockets.serve(service.handler, HOST, PORT, max_size=4096):
        print(f'Entwicklungs-Turnierlobby läuft lokal: ws://{HOST}:{PORT}')
        print('KEINE Verbindung zur Produktion. Noch keine Schachpartien oder Ergebnisse.')
        await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())
