"""Reconnection tests for the local-only, verified tournament WebSocket service."""
import asyncio
import json
import unittest

import websockets

from online_tournament_resume_dev import ResumableVerifiedTournamentWebSocketService


class TournamentReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.svc = ResumableVerifiedTournamentWebSocketService()
        self.server = await websockets.serve(self.svc.handler, '127.0.0.1', 0)
        self.url = 'ws://127.0.0.1:' + str(self.server.sockets[0].getsockname()[1])
        self.connections = []

    async def asyncTearDown(self):
        for ws in self.connections:
            await ws.close()
        self.server.close()
        await self.server.wait_closed()

    async def connect(self):
        ws = await websockets.connect(self.url)
        self.connections.append(ws)
        return ws

    async def expect(self, ws, action):
        for _ in range(50):
            packet = json.loads(await asyncio.wait_for(ws.recv(), 2))
            if packet.get('action') == action:
                return packet
        self.fail('No packet of type ' + action)

    async def send(self, ws, action, **kwargs):
        await ws.send(json.dumps({'action': action, 'code': 'TEST-RESUME', **kwargs}))

    async def register(self, count=4, start=True):
        peers, ids, tokens = [], [], []
        for i in range(count):
            ws = await self.connect()
            peers.append(ws)
            await self.send(ws, 'tournament_create' if i == 0 else 'tournament_join',
                            **({'capacity': count} if i == 0 else {}), name=f'P{i+1}')
            reply = await self.expect(ws, 'tournament_created' if i == 0 else 'tournament_joined')
            creds = await self.expect(ws, 'tournament_resume_credentials')
            ids.append(reply['player_id'])
            tokens.append(creds['token'])
            self.assertEqual(creds['code'], 'TEST-RESUME')
        if start:
            await self.send(peers[0], 'tournament_start')
            await self.expect(peers[0], 'tournament_started')
        return peers, ids, tokens

    async def disconnected(self, code, player_id):
        for _ in range(100):
            if player_id not in self.svc.members.get(code, {}):
                return
            await asyncio.sleep(0.01)
        self.fail('Server did not notice disconnection')

    async def resume(self, player_id, token, code='TEST-RESUME'):
        ws = await self.connect()
        await ws.send(json.dumps({'action':'tournament_resume',
                                  'code':code, 'player_id':player_id, 'token':token}))
        return ws

    async def test_disconnect_and_resume_with_original_board(self):
        p, ids, tokens = await self.register()
        await self.send(p[0], 'tournament_move', match_id='R1M1', move='e2e4')
        await self.expect(p[0], 'tournament_game_update')
        await p[0].close()
        await self.disconnected('TEST-RESUME', ids[0])
        recovered = await self.resume(ids[0], tokens[0])
        result = await self.expect(recovered, 'tournament_resumed')
        self.assertEqual(result['game_states'][0]['moves'], ['e2e4'])
        self.assertEqual(result['game_states'][0]['match_id'], 'R1M1')
        self.assertEqual(result['snapshot']['state'], 'running')
        await self.send(p[1], 'tournament_move', match_id='R1M1', move='e7e5')
        updated = await self.expect(recovered, 'tournament_game_update')
        self.assertEqual(updated['state']['moves'], ['e2e4', 'e7e5'])
        await self.send(recovered, 'tournament_move', match_id='R1M1', move='g1f3')
        self.assertEqual((await self.expect(recovered, 'tournament_game_update'))['state']['moves'],
                         ['e2e4', 'e7e5', 'g1f3'])

    async def test_wrong_token_cannot_take_seat(self):
        p, ids, tokens = await self.register()
        await p[0].close()
        await self.disconnected('TEST-RESUME', ids[0])
        bad = await self.resume(ids[0], tokens[1])
        self.assertEqual((await self.expect(bad, 'tournament_error'))['action'], 'tournament_error')
        self.assertNotIn(bad, self.svc.membership)
        good = await self.resume(ids[0], tokens[0])
        self.assertEqual((await self.expect(good, 'tournament_resumed'))['game_states'][0]['white'], 'P1')

    async def test_cannot_take_an_active_player_seat(self):
        p, ids, tokens = await self.register()
        attacker = await self.resume(ids[0], tokens[0])
        self.assertIn('bereits verbunden', (await self.expect(attacker, 'tournament_error'))['message'])
        self.assertIn(ids[0], self.svc.members['TEST-RESUME'])

    async def test_token_not_in_public_snapshot(self):
        p, ids, tokens = await self.register()
        await self.send(p[2], 'tournament_state')
        snapshot = (await self.expect(p[2], 'tournament_state'))['snapshot']
        encoded = json.dumps(snapshot)
        for token in tokens:
            self.assertNotIn(token, encoded)
        self.assertNotIn('player_id', encoded)
        self.assertNotIn('token', encoded)

    async def test_wrong_room_cannot_resume(self):
        p, ids, tokens = await self.register()
        await p[0].close()
        await self.disconnected('TEST-RESUME', ids[0])
        other = await self.resume(ids[0], tokens[0], code='OTHER-ROOM')
        self.assertEqual((await self.expect(other, 'tournament_error'))['action'], 'tournament_error')
        self.assertNotIn(other, self.svc.membership)

    async def test_cannot_resume_before_start(self):
        p, ids, tokens = await self.register(start=False)
        await p[1].close()
        await self.disconnected('TEST-RESUME', ids[1])
        candidate = await self.resume(ids[1], tokens[1])
        self.assertEqual((await self.expect(candidate, 'tournament_error'))['action'], 'tournament_error')

    async def test_resumed_player_cannot_read_other_match(self):
        p, ids, tokens = await self.register()
        await p[0].close()
        await self.disconnected('TEST-RESUME', ids[0])
        recovered = await self.resume(ids[0], tokens[0])
        await self.expect(recovered, 'tournament_resumed')
        await self.send(recovered, 'tournament_match_state', match_id='R1M2')
        self.assertIn('nicht in dieser Partie', (await self.expect(recovered,'tournament_error'))['message'])

    async def test_repeated_resume_works_after_next_disconnect(self):
        p, ids, tokens = await self.register()
        await p[0].close()
        await self.disconnected('TEST-RESUME', ids[0])
        recovered = await self.resume(ids[0], tokens[0])
        await self.expect(recovered, 'tournament_resumed')
        await recovered.close()
        await self.disconnected('TEST-RESUME', ids[0])
        again = await self.resume(ids[0], tokens[0])
        self.assertEqual((await self.expect(again, 'tournament_resumed'))['snapshot']['state'], 'running')


if __name__ == '__main__':
    unittest.main(verbosity=2)
