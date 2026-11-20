"""Isolated v2 protocol for normal chess games. Not the production entry point."""
import asyncio
import json
import logging
import os
import re
import secrets
import time
from dataclasses import dataclass, field

import chess
import websockets
from websockets.exceptions import ConnectionClosed

GRACE = 120.0
MAX_ROOMS = 128

class ProtocolError(ValueError):
    pass

@dataclass
class Seat:
    token: str
    name: str
    ws: object = None
    lost: float | None = None
    left: bool = False

@dataclass
class Game:
    settings: dict
    created: float
    seats: dict = field(default_factory=dict)
    board: object = field(default_factory=chess.Board)
    white_role: str = 'host'
    generation: int = 0
    revision: int = 0
    remaining: dict = field(default_factory=dict)
    since: float = 0.0
    result: dict | None = None
    offers: set = field(default_factory=set)
    chat: list = field(default_factory=list)
    finished: float | None = None

    def reset(self, now):
        self.board = chess.Board()
        seconds = self.settings['game_minutes'] * 60.0
        self.remaining = {'white': seconds, 'black': seconds}
        self.since = now
        self.result = None
        self.finished = None
        self.offers.clear()

    def color(self, role):
        return 'white' if role == self.white_role else 'black'

    def role(self, color):
        return self.white_role if color == 'white' else ('join' if self.white_role == 'host' else 'host')

    def finish(self, winner, reason, now):
        if self.result is None:
            self.result = {'winner': winner, 'reason': reason}
            self.finished = now
            self.offers.clear()
            self.revision += 1

    def advance(self, now):
        if not self.generation or self.result:
            return
        active = 'white' if self.board.turn else 'black'
        deadlines = [(s.lost + GRACE, r) for r, s in self.seats.items() if s.lost is not None]
        expiry = min((d for d, _ in deadlines), default=float('inf'))
        flag = self.since + self.remaining[active] if self.settings['use_timer'] else float('inf')
        until = min(now, expiry, flag)
        if self.settings['use_timer']:
            self.remaining[active] = max(0., self.remaining[active] - max(0., until - self.since))
        self.since = until
        if flag <= now and flag <= expiry:
            winner = 'black' if active == 'white' else 'white'
            if self.board.has_insufficient_material(winner == 'white'):
                winner = None
            self.finish(winner, 'timeout', flag)
        elif expiry <= now:
            missing = [r for r, s in self.seats.items() if s.lost is not None]
            winner = self.color('join' if missing[0] == 'host' else 'host') if len(missing) == 1 else None
            self.finish(winner, 'disconnect' if winner else 'abandoned', expiry)

    def state(self, now):
        return {'type': 'state', 'generation': self.generation, 'revision': self.revision,
                'settings': self.settings, 'white_role': self.white_role,
                'moves': [m.uci() for m in self.board.move_stack], 'fen': self.board.fen(),
                'clock_ms': {k: max(0, round(v*1000)) for k,v in self.remaining.items()},
                'connected': {r: s.ws is not None for r,s in self.seats.items()},
                'reconnect_ms': {r: max(0, round((s.lost+GRACE-now)*1000)) for r,s in self.seats.items() if s.lost is not None},
                'left': [r for r,s in self.seats.items() if s.left],
                'result': self.result, 'rematch': sorted(self.offers), 'chat': list(self.chat)}

class Service:
    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.rooms = {}
        self.sessions = {}
        self.outboxes = {}
        self.lock = asyncio.Lock()

    def send(self, ws, data):
        q = self.outboxes.get(ws)
        if q is None:
            return
        try:
            q.put_nowait(json.dumps(data, ensure_ascii=False))
        except asyncio.QueueFull:
            asyncio.create_task(ws.close(code=1013, reason='Slow receiver'))

    async def writer(self, ws, q):
        try:
            while True:
                await self.write_one(ws,q)
        except (ConnectionClosed, TimeoutError, OSError):
            await ws.close()

    async def write_one(self, ws, q):
        message = await q.get()
        await asyncio.wait_for(ws.send(message), timeout=3)

    def broadcast(self, game, now):
        state = game.state(now)
        for seat in game.seats.values():
            if seat.ws is not None:
                self.send(seat.ws, state)

    @staticmethod
    def settings(payload):
        if not isinstance(payload, dict):
            raise ProtocolError('settings')
        ranked, timer, minutes, theme = (payload.get(k) for k in ('ranked','use_timer','game_minutes','theme'))
        if type(ranked) is not bool or type(timer) is not bool or type(minutes) is not int or (timer and not 2 <= minutes <= 10) or (not timer and minutes != 0) or theme not in ('Klassisch (Holz)','Modern (Blau)','Wald & Wiese'):
            raise ProtocolError('settings')
        return dict(ranked=ranked,use_timer=timer,game_minutes=minutes,theme=theme)

    def register(self, ws, packet, now):
        room, role = packet.get('room'), packet.get('role')
        if not isinstance(room,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,32}',room) or role not in ('host','join'):
            raise ProtocolError('room_or_role')
        if ws in self.sessions:
            raise ProtocolError('already_registered')
        game = self.rooms.get(room)
        if packet['type'] == 'resume':
            if game is None or role not in game.seats:
                raise ProtocolError('resume_denied')
            seat = game.seats[role]
            token = packet.get('token')
            if seat.left or not isinstance(token,str) or not token.isascii() or not secrets.compare_digest(seat.token,token):
                raise ProtocolError('resume_denied')
            game.advance(now)
            if seat.ws is not None and seat.ws is not ws:
                # A valid secret authorizes replacing a half-open socket. Old cleanup is identity-checked.
                old = seat.ws
                self.sessions.pop(old,None)
                asyncio.create_task(old.close(code=4001,reason='Replaced'))
            seat.ws, seat.lost = ws, None
            self.sessions[ws] = (room,role)
            game.revision += 1
            self.send(ws, {'type':'welcome','role':role,'token':seat.token,'resumed':True})
            self.broadcast(game,now)
            return
        settings = self.settings(packet.get('settings'))
        name = packet.get('name')
        if not isinstance(name,str) or not 1 <= len(name.strip()) <= 15 or any(ord(c)<32 for c in name):
            raise ProtocolError('name')
        if role == 'host':
            if game is not None or len(self.rooms)>=MAX_ROOMS:
                raise ProtocolError('room_unavailable')
            game = self.rooms[room] = Game(settings,now)
        elif game is None or 'join' in game.seats or game.seats['host'].ws is None or game.generation:
            raise ProtocolError('room_unavailable')
        if settings != game.settings:
            raise ProtocolError('settings_mismatch')
        seat = Seat(secrets.token_urlsafe(32),name.strip(),ws)
        game.seats[role] = seat
        self.sessions[ws] = (room,role)
        self.send(ws, {'type':'welcome','role':role,'token':seat.token,'resumed':False})
        if len(game.seats)==2:
            game.generation = 1
            game.reset(now)
        game.revision += 1
        self.broadcast(game,now)

    def dispatch(self, ws, packet, now):
        if not isinstance(packet,dict):
            raise ProtocolError('packet')
        action = packet.get('type')
        if action in ('register','resume'):
            self.register(ws,packet,now)
            return
        if ws not in self.sessions:
            raise ProtocolError('not_registered')
        room, role = self.sessions[ws]
        game = self.rooms[room]
        if game.seats[role].ws is not ws:
            raise ProtocolError('not_registered')
        old = game.revision
        game.advance(now)
        if game.revision != old:
            self.broadcast(game,now)
        if action == 'state_request':
            self.send(ws,game.state(now))
            return
        if action == 'leave':
            if game.generation and game.result is None:
                game.finish(game.color('join' if role=='host' else 'host'),'leave',now)
            game.seats[role].left = True
            self.detach(ws,now)
            # The leaver also receives the final result before the acknowledgement.
            if game.generation:
                self.send(ws,game.state(now))
            # Acknowledge explicit leave only after the result and seat are committed.
            self.send(ws, {'type': 'left_ack'})
            return
        if not game.generation or type(packet.get('generation')) is not int or packet['generation']!=game.generation:
            raise ProtocolError('generation')
        if action == 'chat':
            text = packet.get('text')
            if not isinstance(text,str) or not 1<=len(text.strip())<=300:
                raise ProtocolError('chat')
            game.chat = (game.chat + [{'role':role,'name':game.seats[role].name,'text':text.strip()}])[-30:]
        elif action == 'rematch':
            if not game.result or not all(s.ws is not None for s in game.seats.values()):
                raise ProtocolError('rematch_unavailable')
            game.offers.add(role)
            if len(game.offers)==2:
                game.white_role = secrets.choice(('host','join'))
                game.generation += 1
                game.reset(now)
        elif action == 'resign':
            if game.result:
                raise ProtocolError('game_finished')
            game.finish(game.color('join' if role=='host' else 'host'),'resign',now)
        elif action == 'move':
            if game.result:
                raise ProtocolError('game_not_ready')
            if type(packet.get('ply')) is not int or packet['ply'] != len(game.board.move_stack):
                raise ProtocolError('stale_move')
            if game.color(role) != ('white' if game.board.turn else 'black'):
                raise ProtocolError('wrong_turn')
            try:
                move = chess.Move.from_uci(packet.get('uci',''))
            except (ValueError,TypeError,AttributeError):
                raise ProtocolError('illegal_move') from None
            if move not in game.board.legal_moves:
                raise ProtocolError('illegal_move')
            game.board.push(move)
            outcome = game.board.outcome()
            if outcome:
                game.finish(None if outcome.winner is None else ('white' if outcome.winner else 'black'),outcome.termination.name.lower(),now)
        else:
            raise ProtocolError('unknown_action')
        game.revision += 1
        self.broadcast(game,now)

    def detach(self, ws, now):
        loc = self.sessions.pop(ws,None)
        if not loc:
            return
        room,role = loc
        game = self.rooms.get(room)
        if game is None or game.seats[role].ws is not ws:
            return
        game.advance(now)
        seat = game.seats[role]
        seat.ws,seat.lost = None,now
        game.revision += 1
        if not game.generation:
            self.rooms.pop(room,None)
            return
        self.broadcast(game,now)

    async def handler(self, ws):
        q = self.outboxes[ws] = asyncio.Queue(maxsize=32)
        writer = asyncio.create_task(self.writer(ws,q))
        try:
            async for raw in ws:
                # Also enforce the limit at the application boundary: a first
                # frame may already have been parsed during HTTP upgrade.
                if len(raw.encode("utf-8") if isinstance(raw, str) else raw) > 4096:
                    await ws.close(code=1009, reason="Message too large")
                    return
                async with self.lock:
                    try:
                        self.dispatch(ws,json.loads(raw),self.clock())
                    except (ProtocolError,ValueError,TypeError) as exc:
                        self.send(ws,{'type':'error','code':str(exc) if isinstance(exc,ProtocolError) else 'packet'})
        except ConnectionClosed:
            pass
        finally:
            async with self.lock:
                self.detach(ws,self.clock())
                self.outboxes.pop(ws,None)
            writer.cancel()
            await asyncio.gather(writer,return_exceptions=True)

    async def monitor(self):
        while True:
            await asyncio.sleep(1)
            async with self.lock:
                now = self.clock()
                for room,g in list(self.rooms.items()):
                    g.advance(now)
                    if g.generation:
                        self.broadcast(g,now)
                    if (not g.generation and now-g.created>600) or (g.finished is not None and now-g.finished>600):
                        for seat in g.seats.values():
                            if seat.ws is not None:
                                self.sessions.pop(seat.ws,None)
                                asyncio.create_task(seat.ws.close())
                        self.rooms.pop(room,None)

async def main():
    logging.basicConfig(level=logging.INFO)
    service=Service()
    host='127.0.0.1'  # deliberately local only at this stage
    port=int(os.environ.get('NORMAL_RECONNECT_PORT','10004'))
    async with websockets.serve(service.handler,host,port,max_size=4096,ping_interval=5,ping_timeout=5,close_timeout=2):
        print(f'Normal reconnect TEST: ws://{host}:{port}',flush=True)
        await service.monitor()

if __name__=='__main__':
    asyncio.run(main())
