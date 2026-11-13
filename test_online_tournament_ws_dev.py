"""Real WebSocket tests for the local-only tournament lobby. No Render traffic."""
import asyncio
import json
import unittest
import websockets
from online_tournament_ws_dev import TournamentLobbyService


class LobbyWebSocketTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.svc = TournamentLobbyService()
        self.server = await websockets.serve(self.svc.handler, '127.0.0.1', 0)
        self.url = 'ws://127.0.0.1:' + str(self.server.sockets[0].getsockname()[1])
        self.clients = []

    async def asyncTearDown(self):
        for ws in self.clients:
            await ws.close()
        self.server.close()
        await self.server.wait_closed()

    async def connect(self):
        ws = await websockets.connect(self.url)
        self.clients.append(ws)
        return ws

    async def send_and_get(self, ws, packet, expected_action, predicate=None):
        await ws.send(json.dumps(packet))
        for _ in range(15):
            data = json.loads(await asyncio.wait_for(ws.recv(), 1.5))
            if data['action'] == expected_action and (predicate is None or predicate(data)):
                return data
        self.fail('Antwort nicht erhalten: ' + expected_action)

    async def test_create_join_and_host_start_four(self):
        host = await self.connect()
        created = await self.send_and_get(host, {'action':'tournament_create','code':'abc-1','capacity':4,'name':'A'},'tournament_created')
        self.assertEqual(created['code'], 'ABC-1')
        self.assertEqual(created['snapshot']['state'], 'waiting')
        joiners=[]
        for name in ('B','C','D'):
            ws=await self.connect()
            got=await self.send_and_get(ws,{'action':'tournament_join','code':'abc-1','name':name},'tournament_joined')
            self.assertNotEqual(created['player_id'],got['player_id'])
            joiners.append(ws)
        started=await self.send_and_get(host,{'action':'tournament_start','code':'ABC-1'},'tournament_started')
        self.assertEqual(started['snapshot']['state'],'running')
        self.assertEqual(len(started['snapshot']['rounds'][0]),2)
        self.assertEqual(started['snapshot']['rounds'][0][0]['white'],'A')
        self.assertEqual(started['snapshot']['rounds'][0][0]['black'],'B')
        for ws in joiners:
            data=await self.send_and_get(ws,{'action':'tournament_state','code':'ABC-1'},'tournament_state', lambda d: d['snapshot']['state'] == 'running')
            self.assertEqual(data['snapshot']['state'],'running')

    async def test_only_host_may_start(self):
        host=await self.connect()
        await self.send_and_get(host,{'action':'tournament_create','code':'ROOM1','capacity':4,'name':'A'},'tournament_created')
        others=[]
        for name in ('B','C','D'):
            ws=await self.connect()
            await self.send_and_get(ws,{'action':'tournament_join','code':'ROOM1','name':name},'tournament_joined')
            others.append(ws)
        err=await self.send_and_get(others[0],{'action':'tournament_start','code':'ROOM1'},'tournament_error')
        self.assertIn('Turnierleitung',err['message'])
        self.assertFalse(self.svc.rooms['ROOM1'].started)

    async def test_reject_untrusted_winner_report(self):
        host=await self.connect()
        await self.send_and_get(host,{'action':'tournament_create','code':'ROOM2','capacity':4,'name':'A'},'tournament_created')
        err=await self.send_and_get(host,{'action':'tournament_result','code':'ROOM2','winner':'A'},'tournament_error')
        self.assertIn('nicht erlaubte',err['message'])
        self.assertFalse(self.svc.rooms['ROOM2'].started)

    async def test_nonmember_cannot_read_room(self):
        host=await self.connect()
        await self.send_and_get(host,{'action':'tournament_create','code':'ROOM3','capacity':4,'name':'A'},'tournament_created')
        outsider=await self.connect()
        got=await self.send_and_get(outsider,{'action':'tournament_state','code':'ROOM3'},'tournament_error')
        self.assertIn('nicht Mitglied',got['message'])

    async def test_host_disconnection_cancels_lobby(self):
        host=await self.connect()
        await self.send_and_get(host,{'action':'tournament_create','code':'ROOM4','capacity':4,'name':'A'},'tournament_created')
        join=await self.connect()
        await self.send_and_get(join,{'action':'tournament_join','code':'ROOM4','name':'B'},'tournament_joined')
        await host.close()
        cancelled = json.loads(await asyncio.wait_for(join.recv(),2))
        # Join may have an earlier state broadcast queued.
        if cancelled['action'] != 'tournament_cancelled':
            cancelled = json.loads(await asyncio.wait_for(join.recv(),2))
        self.assertEqual(cancelled['action'], 'tournament_cancelled')
        self.assertNotIn('ROOM4',self.svc.rooms)

    async def test_reject_extra_players(self):
        host=await self.connect()
        await self.send_and_get(host,{'action':'tournament_create','code':'ROOM5','capacity':4,'name':'A'},'tournament_created')
        for name in ('B','C','D'):
            ws=await self.connect()
            await self.send_and_get(ws,{'action':'tournament_join','code':'ROOM5','name':name},'tournament_joined')
        extra=await self.connect()
        got=await self.send_and_get(extra,{'action':'tournament_join','code':'ROOM5','name':'E'},'tournament_error')
        self.assertIn('voll',got['message'])


if __name__=='__main__':
    unittest.main(verbosity=2)
