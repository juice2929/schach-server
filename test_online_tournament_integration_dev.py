"""Local WebSocket integration tests: real chess rules, no Render/network deploy."""
import asyncio
import json
import unittest

import chess
import websockets

from online_tournament_ws_game_dev import VerifiedTournamentWebSocketService


class TournamentOnlineIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.svc = VerifiedTournamentWebSocketService()
        self.server = await websockets.serve(self.svc.handler, '127.0.0.1', 0)
        self.url = 'ws://127.0.0.1:' + str(self.server.sockets[0].getsockname()[1])
        self.sockets = []

    async def asyncTearDown(self):
        for ws in self.sockets:
            await ws.close()
        self.server.close()
        await self.server.wait_closed()

    async def connect(self):
        ws = await websockets.connect(self.url)
        self.sockets.append(ws)
        return ws

    async def expect(self, ws, action, predicate=None):
        # Lobby and game updates may be queued in between explicit responses.
        for _ in range(80):
            message = json.loads(await asyncio.wait_for(ws.recv(), 2))
            if message.get('action') == action and (predicate is None or predicate(message)):
                return message
        self.fail(f'Did not receive {action}')

    async def call(self, ws, action, predicate=None, **kwargs):
        await ws.send(json.dumps({'action': action, 'code': 'ONLINE-TEST', **kwargs}))
        reply = {'tournament_create': 'tournament_created',
                 'tournament_join': 'tournament_joined',
                 'tournament_start': 'tournament_started'}.get(action, action)
        return await self.expect(ws, reply, predicate)

    async def setup_room(self, size=4):
        clients = [await self.connect() for _ in range(size)]
        await self.call(clients[0], 'tournament_create', capacity=size, name='P1')
        for i, ws in enumerate(clients[1:], start=2):
            await self.call(ws, 'tournament_join', name=f'P{i}')
        await self.call(clients[0], 'tournament_start')
        return clients

    async def game(self, ws, action, match='R1M1', expected_ply=None, expected_event=None, **data):
        await ws.send(json.dumps({'action': action, 'code': 'ONLINE-TEST', 'match_id': match, **data}))
        reply = 'tournament_match_state' if action == 'tournament_match_state' else 'tournament_game_update'
        def target(packet):
            state = packet['state']
            return (state['match_id'] == match
                    and (expected_ply is None or len(state['moves']) == expected_ply)
                    and (expected_event is None or packet.get('event') == expected_event))
        return await self.expect(ws, reply, target)

    async def error(self, ws, action, match='R1M1', **data):
        await ws.send(json.dumps({'action': action, 'code': 'ONLINE-TEST', 'match_id': match, **data}))
        return await self.expect(ws, 'tournament_error')

    async def test_full_four_player_tournament_via_websockets(self):
        p = await self.setup_room()
        first = await self.game(p[1], 'tournament_resign')  # P2 resigns, P1 advances
        self.assertEqual(first['state']['winner'], 'P1')
        second = await self.game(p[3], 'tournament_resign', match='R1M2')  # P4 resigns
        self.assertEqual(second['state']['winner'], 'P3')
        final = await self.game(p[2], 'tournament_resign', match='R2M1')  # P3 resigns
        self.assertEqual(final['state']['winner'], 'P1')
        snapshot = await self.call(p[0], 'tournament_state', lambda d: d['snapshot']['state'] == 'finished')
        self.assertEqual(snapshot['snapshot']['champion'], 'P1')
        self.assertEqual(snapshot['snapshot']['state'], 'finished')

    async def test_full_eight_player_tournament_via_websockets(self):
        p = await self.setup_room(8)
        for match, loser in [('R1M1',1),('R1M2',3),('R1M3',5),('R1M4',7),
                             ('R2M1',2),('R2M2',6),('R3M1',4)]:
            await self.game(p[loser], 'tournament_resign', match=match)
        snapshot = await self.call(p[0], 'tournament_state', lambda d: d['snapshot']['state'] == 'finished')
        self.assertEqual(snapshot['snapshot']['champion'], 'P1')
        self.assertEqual(len(snapshot['snapshot']['rounds']), 3)

    async def test_server_legally_checks_moves_and_updates_both_players(self):
        p = await self.setup_room()
        result = await self.game(p[0], 'tournament_move', move='e2e4', expected_ply=1)
        self.assertEqual(result['event'], 'move_accepted')
        self.assertEqual(result['state']['moves'], ['e2e4'])
        other = await self.expect(p[1], 'tournament_game_update')
        self.assertEqual(other['state']['moves'], ['e2e4'])
        self.assertEqual((await self.game(p[1], 'tournament_match_state'))['state']['turn'], 'black')
        self.assertEqual((await self.game(p[2], 'tournament_match_state', match='R1M2'))['state']['moves'], [])

    async def test_wrong_turn_and_illegal_move_are_rejected_without_changes(self):
        p = await self.setup_room()
        wrong = await self.error(p[1], 'tournament_move', move='e7e5')
        self.assertIn('nicht am Zug', wrong['message'])
        illegal = await self.error(p[0], 'tournament_move', move='e2e5')
        self.assertIn('Unzulässiger', illegal['message'])
        self.assertEqual((await self.game(p[0], 'tournament_match_state'))['state']['moves'], [])

    async def test_unrelated_player_cannot_move_or_read_game(self):
        p = await self.setup_room()
        for action in ('tournament_match_state', 'tournament_move', 'tournament_resign'):
            err = await self.error(p[2], action, move='e2e4')
            self.assertIn('nicht in dieser Partie', err['message'])

    async def test_forged_winner_and_player_ids_are_rejected(self):
        p = await self.setup_room()
        forged = await self.error(p[0], 'tournament_resign', winner='P1')
        self.assertIn('nicht vom Client', forged['message'])
        forged = await self.error(p[0], 'tournament_move', move='e2e4', player_id='other')
        self.assertIn('nicht vom Client', forged['message'])
        await p[0].send(json.dumps({'action':'tournament_result','code':'ONLINE-TEST','winner':'P1'}))
        err = await self.expect(p[0], 'tournament_error')
        self.assertIn('nicht erlaubte', err['message'])
        self.assertIsNone(self.svc.rooms['ONLINE-TEST']._get_match('R1M1').winner_id)

    async def test_checkmate_is_adjudicated_by_server(self):
        p = await self.setup_room()
        sequence = [(0,'f2f3'),(1,'e7e5'),(0,'g2g4'),(1,'d8h4')]
        for ply, (player, move) in enumerate(sequence, 1):
            result = await self.game(p[player], 'tournament_move', move=move, expected_ply=ply)
        self.assertEqual(result['event'], 'game_finished')
        self.assertEqual(result['state']['reason'], 'CHECKMATE')
        self.assertEqual(result['state']['winner'], 'P2')
        self.assertEqual(self.svc.rooms['ONLINE-TEST']._get_match('R2M1').white_id,
                         self.svc.rooms['ONLINE-TEST']._get_match('R1M1').winner_id)
        err = await self.error(p[0], 'tournament_move', move='e2e4')
        self.assertIn('bereits beendet', err['message'])

    async def test_draw_claim_restarts_match_with_empty_board(self):
        p = await self.setup_room()
        sequence = [(0,'g1f3'),(1,'g8f6'),(0,'f3g1'),(1,'f6g8')] * 2
        for ply, (player, move) in enumerate(sequence, 1):
            await self.game(p[player], 'tournament_move', move=move, expected_ply=ply)
        res = await self.game(p[0], 'tournament_claim_draw', expected_event='draw_replay')
        self.assertEqual(res['event'], 'draw_replay')
        state = await self.game(p[0], 'tournament_match_state')
        self.assertEqual(state['state']['moves'], [])
        self.assertEqual(state['state']['fen'], chess.Board().fen())
        self.assertEqual(state['state']['draw_replays'], 1)

    async def test_duplicate_action_does_not_advance_twice(self):
        p = await self.setup_room()
        await self.game(p[1], 'tournament_resign')
        err = await self.error(p[1], 'tournament_resign')
        self.assertIn('bereits beendet', err['message'])
        self.assertEqual(self.svc.rooms['ONLINE-TEST']._get_match('R2M1').white_id,
                         self.svc.rooms['ONLINE-TEST']._get_match('R1M1').winner_id)

    async def test_unauthorized_room_cannot_read_match(self):
        p = await self.setup_room()
        outsider = await self.connect()
        await outsider.send(json.dumps({'action':'tournament_match_state', 'code':'ONLINE-TEST', 'match_id':'R1M1'}))
        err = await self.expect(outsider, 'tournament_error')
        self.assertIn('nicht Mitglied', err['message'])

    async def test_bad_match_id_rejected(self):
        p = await self.setup_room()
        err = await self.error(p[0], 'tournament_match_state', match='../R1M1')
        self.assertIn('Ungültige', err['message'])

    async def test_concurrent_moves_are_serialized(self):
        p = await self.setup_room()
        msg = json.dumps({'action':'tournament_move','code':'ONLINE-TEST', 'match_id':'R1M1','move':'e2e4'})
        await asyncio.gather(p[0].send(msg), p[0].send(msg))
        # Same player must be unable to play twice: one accepted and one rejected.
        got = set()
        for _ in range(2):
            while True:
                packet = json.loads(await asyncio.wait_for(p[0].recv(), 2))
                if packet['action'] in ('tournament_game_update','tournament_error'):
                    got.add(packet['action'])
                    break
        self.assertEqual(got, {'tournament_game_update', 'tournament_error'})
        self.assertEqual((await self.game(p[0], 'tournament_match_state'))['state']['moves'], ['e2e4'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
