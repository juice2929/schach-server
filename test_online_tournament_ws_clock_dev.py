import asyncio
import json
import unittest
import websockets
import test_online_tournament_resume_dev as baseline
from online_tournament_ws_clock_dev import TimedTournamentWebSocketService

S = 1_000_000_000


class TimedSocketTests(baseline.TournamentReconnectTests):
    # Re-run the eight previous reconnection tests on the new adapter too.
    async def asyncSetUp(self):
        self.t = 0
        self.time_option = 0
        self.svc = TimedTournamentWebSocketService(lambda: self.t)
        self.server = await websockets.serve(self.svc.handler, '127.0.0.1', 0)
        self.url = 'ws://127.0.0.1:' + str(self.server.sockets[0].getsockname()[1])
        self.connections = []

    async def send(self, ws, action, **kwargs):
        if action == 'tournament_create' and self.time_option:
            kwargs.setdefault('time_minutes', self.time_option)
        await super().send(ws, action, **kwargs)

    async def timed(self, count=4):
        self.time_option = 2
        return await self.register(count=count)

    async def test_clock_configuration_and_private_state(self):
        p, _, _ = await self.timed()
        await self.send(p[0], 'tournament_state')
        # There may be queued older lobby snapshots; all have the same setting.
        self.assertEqual((await self.expect(p[0], 'tournament_state'))['snapshot']['time_minutes'], 2)
        self.t = 3*S
        await self.send(p[0], 'tournament_match_state', match_id='R1M1')
        clock = (await self.expect(p[0], 'tournament_match_state'))['state']['clock']
        self.assertEqual((clock['white_ms'], clock['black_ms'], clock['active']), (117000,120000,'white'))
        await self.send(p[2], 'tournament_match_state', match_id='R1M1')
        await self.expect(p[2], 'tournament_error')

    async def test_tick_only_own_match_and_switch(self):
        p, _, _ = await self.timed()
        self.t = 4*S
        await self.send(p[0], 'tournament_move', match_id='R1M1', move='e2e4')
        a = await self.expect(p[0], 'tournament_game_update')
        self.assertEqual(a['state']['clock']['active'], 'black')
        self.t = 7*S
        await self.svc.poll_once(True)
        for i in (0,1,2,3):
            pkt = await self.expect(p[i], 'tournament_clock')
            self.assertEqual(pkt['match_id'], 'R1M1' if i<2 else 'R1M2')
            self.assertNotIn('moves', pkt)
            if i<2:
                self.assertEqual(pkt['clock']['black_ms'],117000)
                self.assertEqual(pkt['clock']['white_ms'],116000)
            else:
                self.assertEqual(pkt['clock']['white_ms'],113000)

    async def test_monitor_timeout_and_final(self):
        p, _, _ = await self.timed()
        worker = asyncio.create_task(self.svc.monitor(.005, 1))
        try:
            self.t = 120*S
            for i in (0,2):
                pkt = await self.expect(p[i], 'tournament_game_update')
                self.assertEqual(pkt['state']['reason'],'TIMEOUT')
                self.assertEqual(pkt['state']['winner'], 'P2' if i==0 else 'P4')
            self.t = 240*S
            pkt = await self.expect(p[1], 'tournament_game_update')  # queued semifinal result
            self.assertEqual(pkt['state']['match_id'],'R1M1')
            final = await self.expect(p[1], 'tournament_game_update')
            self.assertEqual(final['state']['match_id'],'R2M1')
            self.assertEqual(final['state']['winner'],'P4')
            self.assertEqual(self.svc.rooms['TEST-RESUME'].snapshot()['champion'],'P4')
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

    async def test_resume_preserves_clock(self):
        p, ids, tokens = await self.timed()
        self.t = 5*S
        await p[0].close()
        await self.disconnected('TEST-RESUME',ids[0])
        self.t = 11*S
        ws = await self.resume(ids[0],tokens[0])
        reply = await self.expect(ws,'tournament_resumed')
        self.assertEqual(reply['game_states'][0]['clock']['white_ms'],109000)

    async def test_invalid_time_no_room(self):
        ws = await self.connect()
        for invalid in (-1, 1, 60, True, '5'):
            await self.send(ws,'tournament_create',capacity=4,name='P1',time_minutes=invalid)
            await self.expect(ws,'tournament_error')
            self.assertNotIn('TEST-RESUME',self.svc.rooms)

    async def test_untimed_no_clock(self):
        p, _, _ = await self.register()
        self.t = 100000*S
        await self.svc.poll_once(True)
        await self.send(p[0],'tournament_move',match_id='R1M1',move='e2e4')
        pkt = await self.expect(p[0],'tournament_game_update')
        self.assertNotIn('clock',pkt['state'])
        self.assertEqual(pkt['event'],'move_accepted')

    async def test_draw_replay_resets_clock(self):
        p, _, _ = await self.timed()
        for i,m in enumerate(['g1f3','g8f6','f3g1','f6g8']*2):
            self.t += S
            await self.send(p[i%2],'tournament_move',match_id='R1M1',move=m)
            for j in (0,1):
                await self.expect(p[j],'tournament_game_update')
        await self.send(p[0],'tournament_claim_draw',match_id='R1M1')
        self.assertEqual((await self.expect(p[0],'tournament_game_update'))['event'],'draw_replay')
        await self.send(p[0],'tournament_match_state',match_id='R1M1')
        state = (await self.expect(p[0],'tournament_match_state'))['state']
        self.assertEqual(state['moves'],[])
        self.assertEqual(state['clock']['white_ms'],120000)
        self.assertEqual(state['clock']['black_ms'],120000)

if __name__ == '__main__':
    unittest.main(verbosity=2)
