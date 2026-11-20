"""Real localhost WebSocket/chess regression tests. Run with pinned requirements."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch
import websockets
from test_online_tournament_ws_clock_dev import TimedSocketTests as _Baseline
from online_tournament_reliable_dev import ReliableTournamentService
S=1_000_000_000
CODE='TEST-RESUME'

class ReliabilityTests(_Baseline):
    async def asyncSetUp(self):
        self.t=0; self.time_option=0
        self.svc=ReliableTournamentService(lambda:self.t)
        self.server=await websockets.serve(self.svc.handler,'127.0.0.1',0,ping_interval=5,ping_timeout=5,close_timeout=2)
        self.url='ws://127.0.0.1:'+str(self.server.sockets[0].getsockname()[1]); self.connections=[]

    async def event(self, ws, name):
        for _ in range(100):
            pkt=json.loads(await asyncio.wait_for(ws.recv(),3))
            if pkt.get('action')=='tournament_game_update' and pkt.get('event')==name:
                return pkt['state']
        self.fail('No game event '+name)

    async def test_resume_preserves_clock(self):
        p,ids,tokens=await self.timed()
        self.t=5*S; await p[0].close(); await self.disconnected(CODE,ids[0])
        paused=await self.event(p[1],'clock_paused')
        self.assertEqual(paused['clock']['white_ms'],115000)
        self.assertIsNone(paused['clock']['active'])
        self.t=11*S; ws=await self.resume(ids[0],tokens[0]); await self.expect(ws,'tournament_resumed')
        c=(await self.event(ws,'clock_resumed'))['clock']
        self.assertEqual((c['white_ms'],c['black_ms'],c['active']),(115000,120000,'white'))
        self.t=12*S; await self.send(ws,'tournament_move',match_id='R1M1',move='e2e4')
        state=await self.event(ws,'move_accepted'); self.assertEqual(state['clock']['white_ms'],114000)
        self.assertEqual(state['moves'],['e2e4'])

    async def test_pause_only_affected_match_and_reject_move_without_reconnect_error(self):
        p,ids,tokens=await self.timed(8); self.t=2*S
        await p[1].close(); await self.disconnected(CODE,ids[1]); await self.event(p[0],'clock_paused')
        self.t=10*S
        await self.send(p[0],'tournament_move',match_id='R1M1',move='e2e4')
        self.assertEqual((await self.event(p[0],'clock_paused'))['moves'],[])
        await self.send(p[2],'tournament_move',match_id='R1M2',move='d2d4')
        s=await self.event(p[2],'move_accepted'); self.assertEqual(s['clock']['white_ms'],110000)
        self.assertEqual(s['moves'],['d2d4'])

    async def test_budget_shared_across_repeated_disconnects(self):
        p,ids,tokens=await self.timed(); self.t=S
        await p[0].close(); await self.disconnected(CODE,ids[0]); await self.event(p[1],'clock_paused')
        self.t=71*S; ws=await self.resume(ids[0],tokens[0]); await self.expect(ws,'tournament_resumed'); await self.event(ws,'clock_resumed')
        self.t=72*S; await ws.close(); await self.disconnected(CODE,ids[0]); await self.event(p[1],'clock_paused')
        self.t=125*S; await self.svc.poll_once(); s=await self.event(p[1],'clock_resumed')
        self.assertEqual(s['clock']['white_ms'],115000) # 2s before pause + 3s after budget
        self.assertEqual(self.svc.budgets[(CODE,'R1M1')],0)
        self.t=126*S; ws=await self.resume(ids[0],tokens[0]); r=await self.expect(ws,'tournament_resumed')
        self.assertEqual(r['game_states'][0]['clock']['white_ms'],114000)

    async def test_two_missing_peers_resume_only_after_both_back(self):
        p,ids,tokens=await self.timed(); self.t=S
        await p[0].close(); await self.disconnected(CODE,ids[0]); await p[1].close(); await self.disconnected(CODE,ids[1])
        self.t=10*S; a=await self.resume(ids[0],tokens[0]); r=await self.expect(a,'tournament_resumed')
        self.assertIsNone(r['game_states'][0]['clock']['active'])
        self.t=20*S; b=await self.resume(ids[1],tokens[1]); await self.expect(b,'tournament_resumed')
        self.assertEqual((await self.event(a,'clock_resumed'))['clock']['white_ms'],119000)

    async def test_valid_token_can_replace_simulated_zombie_without_late_cleanup(self):
        p,ids,tokens=await self.timed(); old=self.svc.members[CODE][ids[0]]
        old.ping=AsyncMock(side_effect=TimeoutError)
        bad=await self.resume(ids[0],tokens[1]); await self.expect(bad,'tournament_error'); old.ping.assert_not_called()
        ws=await self.resume(ids[0],tokens[0]); await self.expect(ws,'tournament_resumed')
        new=self.svc.members[CODE][ids[0]]; self.assertIsNot(new,old)
        await self.svc.disconnect(old); self.assertIs(self.svc.members[CODE][ids[0]],new)
        await self.send(ws,'tournament_move',match_id='R1M1',move='e2e4'); self.assertEqual((await self.event(ws,'move_accepted'))['moves'],['e2e4'])

    async def test_cannot_take_an_active_player_seat(self):
        # TEST13 policy: only the matching private token permits takeover.
        # The inherited test uses the *correct* token and expects rejection,
        # which remains the policy of the older adapters, not this adapter.
        p, ids, tokens = await self.timed()
        old = self.svc.members[CODE][ids[0]]
        bad = await self.resume(ids[0], tokens[1])
        await self.expect(bad, 'tournament_error')
        self.assertIs(self.svc.members[CODE][ids[0]], old)
        self.assertNotIn(old, self.svc.retired)
        await self.send(p[0], 'tournament_move', match_id='R1M1', move='e2e4')
        self.assertEqual((await self.event(p[0], 'move_accepted'))['moves'], ['e2e4'])

    async def test_responsive_old_final_seat_replaced_and_late_actions_ignored(self):
        p, ids, tokens = await self.timed()
        for i, mid in ((1, 'R1M1'), (3, 'R1M2')):
            await self.send(p[i], 'tournament_resign', match_id=mid)
            await self.event(p[i], 'game_finished')
        await self.send(p[0], 'tournament_move', match_id='R2M1', move='e2e4')
        await self.event(p[0], 'move_accepted')
        self.t = 7*S
        old = self.svc.members[CODE][ids[2]]
        # Real responsive WebSocket; simulate a transport that takes time to
        # disappear, even after authority is revoked.
        pong = await p[2].ping()
        await asyncio.wait_for(pong, 2)
        with patch.object(self.svc, '_abort', return_value=None) as abort:
            bad = await self.resume(ids[2], tokens[0])
            await self.expect(bad, 'tournament_error')
            abort.assert_not_called()
            new = await self.resume(ids[2], tokens[2])
            reply = await self.expect(new, 'tournament_resumed')
            abort.assert_called_once_with(old)
            state = reply['game_states'][0]
            self.assertEqual(state['match_id'], 'R2M1')
            self.assertEqual(state['moves'], ['e2e4'])
            self.assertEqual((state['clock']['white_ms'], state['clock']['black_ms'], state['clock']['active']), (120000, 113000, 'black'))
            self.assertNotIn((CODE, 'R2M1'), self.svc.pauses)
            current = self.svc.members[CODE][ids[2]]
            self.assertNotIn(old, self.svc.membership)
            self.assertIn(old, self.svc.retired)
            await self.svc.process(old, {'action':'tournament_resign','code':CODE,'match_id':'R2M1'})
            await self.svc.process(old, {'action':'tournament_create','code':'RETIRED','capacity':4,'name':'old'})
            self.assertNotIn('RETIRED', self.svc.rooms)
            await self.svc.disconnect(old)
            self.assertIs(self.svc.members[CODE][ids[2]], current)
            await self.send(new, 'tournament_move', match_id='R2M1', move='e7e5')
            self.assertEqual((await self.event(new, 'move_accepted'))['moves'], ['e2e4','e7e5'])
            await self.send(new, 'tournament_resign', match_id='R2M1')
            result = await self.event(new, 'game_finished')
            self.assertEqual(result['winner'], 'P1')
            self.assertEqual(self.svc.rooms[CODE].snapshot()['champion'], 'P1')
        await p[2].close()
        self.assertIs(self.svc.members[CODE][ids[2]], current)

    async def test_responsive_seat_replaced_twice_without_old_cleanup(self):
        p, ids, tokens = await self.timed()
        old = self.svc.members[CODE][ids[0]]
        a = await self.resume(ids[0], tokens[0])
        await self.expect(a, 'tournament_resumed')
        await asyncio.wait_for(p[0].wait_closed(), 3)
        prior = self.svc.members[CODE][ids[0]]
        b = await self.resume(ids[0], tokens[0])
        await self.expect(b, 'tournament_resumed')
        await asyncio.wait_for(a.wait_closed(), 3)
        current = self.svc.members[CODE][ids[0]]
        await self.svc.disconnect(old)
        await self.svc.disconnect(prior)
        self.assertIs(self.svc.members[CODE][ids[0]], current)
        await self.send(b, 'tournament_move', match_id='R1M1', move='e2e4')
        self.assertEqual((await self.event(b, 'move_accepted'))['moves'], ['e2e4'])

    async def test_slow_writer_does_not_hold_dispatch(self):
        p,ids,_=await self.timed(); slow=self.svc.members[CODE][ids[0]]
        gate=asyncio.Event(); entered=asyncio.Event()
        async def stall(*args,**kwargs): entered.set(); await gate.wait()
        slow.send=stall
        await self.svc.send(slow,{'action':'test_stall'})
        await asyncio.wait_for(entered.wait(),1)
        try:
            await self.send(p[2],'tournament_move',match_id='R1M2',move='e2e4')
            await asyncio.wait_for(self.event(p[2],'move_accepted'),1)
        finally: gate.set()

if __name__=='__main__': unittest.main(verbosity=2)
