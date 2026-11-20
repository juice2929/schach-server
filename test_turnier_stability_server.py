"""Real server/chess checks, without changing the Render service."""
import asyncio
import json
import unittest
import websockets
from online_tournament_reliable_dev import ReliableTournamentService

class FinalTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.svc=ReliableTournamentService()
        self.server=await websockets.serve(self.svc.handler,'127.0.0.1',0,ping_interval=5,ping_timeout=5,close_timeout=2)
        self.url='ws://127.0.0.1:'+str(self.server.sockets[0].getsockname()[1]); self.peers=[]
    async def asyncTearDown(self):
        for ws in self.peers: await ws.close()
        self.server.close(); await self.server.wait_closed()
    async def connect(self):
        ws=await websockets.connect(self.url); self.peers.append(ws); return ws
    async def send(self,ws,action,**fields):
        await ws.send(json.dumps({'action':action,'code':'TEST12',**fields}))
    async def expect(self,ws,action,**fields):
        async with asyncio.timeout(5):
            while True:
                p=json.loads(await ws.recv())
                if p.get('action')==action and all(p.get(k)==v for k,v in fields.items()): return p
    async def register(self):
        peers=[]; ids=[]; tokens=[]
        for i in range(4):
            ws=await self.connect(); peers.append(ws)
            await self.send(ws,'tournament_create' if i==0 else 'tournament_join',name=f'P{i+1}',**({'capacity':4,'time_minutes':30} if i==0 else {}))
            p=await self.expect(ws,'tournament_created' if i==0 else 'tournament_joined'); ids.append(p['player_id'])
            tokens.append((await self.expect(ws,'tournament_resume_credentials'))['token'])
        await self.send(peers[0],'tournament_start'); await self.expect(peers[0],'tournament_started')
        for i,mid in ((1,'R1M1'),(3,'R1M2')):
            await self.send(peers[i],'tournament_resign',match_id=mid)
            p=await self.expect(peers[i],'tournament_game_update',event='game_finished')
            self.assertEqual(p['state']['reason'],'RESIGNATION')
        return peers,ids,tokens
    async def resume(self,old,pid,token):
        await old.close()
        async with asyncio.timeout(5):
            while pid in self.svc.members['TEST12']: await asyncio.sleep(.01)
        ws=await self.connect(); await self.send(ws,'tournament_resume',player_id=pid,token=token)
        p=await self.expect(ws,'tournament_resumed'); self.assertEqual(p['game_states'][0]['match_id'],'R2M1')
        return ws,p
    async def exercise(self,resigning_index):
        p,ids,tokens=await self.register()
        a,b=p[0],p[2]
        await self.send(a,'tournament_move',match_id='R2M1',move='e2e4')
        self.assertEqual((await self.expect(a,'tournament_game_update',event='move_accepted'))['state']['moves'],['e2e4'])
        a,reply=await self.resume(a,ids[0],tokens[0]); self.assertEqual(reply['game_states'][0]['moves'],['e2e4'])
        b,reply=await self.resume(b,ids[2],tokens[2]); self.assertEqual(reply['game_states'][0]['moves'],['e2e4'])
        with self.assertLogs('tournament.reliability',level='INFO') as logs:
            await self.send(a,'tournament_move',match_id='R2M1',move='d2d4')
            self.assertIn('nicht am Zug',(await self.expect(a,'tournament_error'))['message'])
            await self.send(b,'tournament_move',match_id='R2M1',move='e7e5')
            self.assertEqual((await self.expect(b,'tournament_game_update',event='move_accepted'))['state']['moves'],['e2e4','e7e5'])
            await self.send((a,b)[resigning_index],'tournament_resign',match_id='R2M1')
            for ws in (a,b):
                r=await self.expect(ws,'tournament_game_update',event='game_finished')
                self.assertEqual(r['state']['reason'],'RESIGNATION'); self.assertEqual(r['state']['winner'],('P3','P1')[resigning_index])
                # Drain older snapshots; authoritative final bracket must name champion.
                async with asyncio.timeout(5):
                    while True:
                        data=await self.expect(ws,'tournament_state')
                        if data['snapshot'].get('champion'): break
                self.assertEqual(data['snapshot']['champion'],('P3','P1')[resigning_index])
        self.assertTrue(any('protocol error (not_turn)' in l for l in logs.output))
        self.assertTrue(any('resign request processed' in l for l in logs.output))
        for token in tokens: self.assertNotIn(token,'\n'.join(logs.output))
    async def test_both_finalists_resume_then_white_resigns(self): await self.exercise(0)
    async def test_both_finalists_resume_then_black_resigns(self): await self.exercise(1)
