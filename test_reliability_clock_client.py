"""Dependency-free tests for pause primitives and actual released client parsing."""
import ast
import os
from pathlib import Path
import unittest
from online_tournament_clock_dev import MatchClock
S=1_000_000_000

class PauseTests(unittest.TestCase):
    def test_pause_and_resume_white(self):
        c=MatchClock(2,0); self.assertTrue(c.pause(5*S))
        self.assertEqual(c.snapshot(90*S),{'minutes':2,'white_ms':115000,'black_ms':120000,'active':None,'expired':None})
        self.assertFalse(c.pause(90*S)); self.assertTrue(c.resume(90*S))
        self.assertEqual(c.snapshot(92*S)['white_ms'],113000)
        self.assertEqual(c.snapshot(92*S)['active'],'white')
    def test_pause_black_and_no_moves(self):
        c=MatchClock(2,0); c.accept_move('white',S); c.pause(3*S)
        with self.assertRaises(ValueError): c.accept_move('black',4*S)
        self.assertEqual(c.snapshot(10*S)['black_ms'],118000)
        c.resume(10*S); c.accept_move('black',12*S)
        self.assertEqual(c.snapshot(12*S)['black_ms'],116000)
        self.assertEqual(c.snapshot(12*S)['active'],'white')
    def test_flag_before_disconnect(self):
        c=MatchClock(2,0); self.assertFalse(c.pause(120*S))
        self.assertEqual(c.expired_side(121*S),'white')
    def test_stop_while_paused(self):
        c=MatchClock(2,0); c.pause(S); c.stop(2*S)
        self.assertFalse(c.resume(3*S)); self.assertFalse(c.paused)
        self.assertEqual(c.snapshot(200*S)['white_ms'],119000)
    def test_untimed(self):
        self.assertFalse(MatchClock(0,0).pause(S))
    def test_independent_clocks(self):
        a,b=MatchClock(2,0),MatchClock(2,0); a.pause(S)
        self.assertEqual(a.snapshot(10*S)['white_ms'],119000)
        self.assertEqual(b.snapshot(10*S)['white_ms'],110000)
    def test_late_poll_budget_end(self):
        c=MatchClock(2,0); c.pause(S); c.resume(121*S)
        self.assertEqual(c.snapshot(125*S)['white_ms'],115000)

class ClientProtocolTests(unittest.TestCase):
    def test_unchanged_client_accepts_full_paused_state_and_resumed_ticks(self):
        path=os.environ.get('TOURNAMENT_CLIENT_SOURCE')
        if not path:
            self.skipTest('Set TOURNAMENT_CLIENT_SOURCE to the original Windows client.')
        module=ast.parse(Path(path).read_text(encoding='utf-8'))
        nodes=[n for n in module.body if (isinstance(n,ast.FunctionDef) and n.name in ('_valid_clock','_apply_clock_sample')) or (isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='TIME_OPTIONS' for t in n.targets))]
        ns={}; exec(compile(ast.Module(body=nodes,type_ignores=[]),'<original client>','exec'),ns)
        c=MatchClock(2,0); c.pause(5*S)
        paused=c.snapshot(10*S)
        self.assertTrue(ns['_valid_clock'](paused))
        state={'match_id':'R1M1','draw_replays':0,'turn':'white','moves':[],'clock':paused}
        packet={'code':'TEST','match_id':'R1M1','draw_replays':0,'clock':paused}
        self.assertIs(ns['_apply_clock_sample'](state,packet,'TEST'),state)
        c.resume(10*S); packet['clock']=c.snapshot(11*S)
        self.assertEqual(ns['_apply_clock_sample'](state,packet,'TEST')['clock']['white_ms'],114000)
        # Verify existing full state handler assigns state, not clock-only filter.
        fn=next(n for n in module.body if isinstance(n,ast.FunctionDef) and n.name=='show_online_tournament')
        handlers=[n for n in ast.walk(fn) if isinstance(n,ast.If) and ast.unparse(n.test)=="action == 'tournament_game_update'"]
        self.assertEqual(len(handlers),1)
        self.assertTrue(any(isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='game_state' for t in n.targets) and isinstance(n.value,ast.Name) and n.value.id=='state' for n in ast.walk(handlers[0])))

class PolicySimulationTests(unittest.IsolatedAsyncioTestCase):
    """Run the actual candidate pause method with fake transport/game adapter.
    No chess-rule or real-network test is implied by these tests.
    """
    async def asyncSetUp(self):
        import logging
        from online_tournament_core import TournamentRoom
        from online_tournament_clock_dev import TournamentClockBook
        self.t=0
        class FakeGames:
            def __init__(self, room, now):
                self.clocks=TournamentClockBook(2,now); self.clocks.sync(room,0)
            def state(self,mid,pid): return {'match_id':mid,'clock':self.clocks.get(mid).snapshot(self.clocks.now())}
        class FakeParent:
            pass
        ns={'TimedTournamentWebSocketService':FakeParent,'TimedTournamentGames':FakeGames,
            'PAUSE_NS':120*S,'monotonic_ns':lambda:0,'LOG':logging.getLogger('simulation')}
        mod=ast.parse(Path(__file__).with_name('online_tournament_reliable_dev.py').read_text(encoding='utf-8'))
        cls=next(n for n in mod.body if isinstance(n,ast.ClassDef) and n.name=='ReliableTournamentService')
        exec(compile(ast.Module(body=[cls],type_ignores=[]),'<actual server candidate>','exec'),ns)
        obj=ns['ReliableTournamentService'].__new__(ns['ReliableTournamentService'])
        self.room=TournamentRoom('SIM',4,'p1','P1')
        for i in (2,3,4): self.room.register(f'p{i}',f'P{i}')
        self.room.start('p1')
        self.games=FakeGames(self.room,lambda:self.t)
        obj.now=lambda:self.t; obj.pause_ns=120*S; obj.pauses={}; obj.budgets={}
        obj.rooms={'SIM':self.room}; obj.verified_games={'SIM':self.games}
        obj.members={'SIM':{f'p{i}':f'ws{i}' for i in (1,2,3,4)}}
        self.sent=[]
        async def send(ws,payload): self.sent.append((ws,payload))
        obj.send=send; self.s=obj
    def clock(self,mid='R1M1'): return self.games.clocks.get(mid)
    async def test_pause_only_private_match(self):
        self.t=5*S; self.s.members['SIM'].pop('p1'); await self.s._sync_pauses()
        self.assertTrue(self.clock().paused); self.assertFalse(self.clock('R1M2').paused)
        self.assertEqual([ws for ws,_ in self.sent],['ws2'])
        self.t=35*S; self.s.members['SIM']['p1']='new1'; await self.s._sync_pauses()
        self.assertEqual(self.clock().snapshot(self.t)['white_ms'],115000)
        self.assertEqual(self.s.budgets['SIM','R1M1'],90*S)
        self.assertEqual(self.clock('R1M2').snapshot(self.t)['white_ms'],85000)
    async def test_late_expiry_and_no_fresh_budget(self):
        self.t=S; self.s.members['SIM'].pop('p1'); await self.s._sync_pauses()
        self.t=131*S; await self.s._sync_pauses()
        self.assertEqual(self.clock().snapshot(self.t)['white_ms'],109000)
        self.assertEqual(self.s.budgets['SIM','R1M1'],0)
        await self.s._sync_pauses(); self.assertFalse(self.clock().paused)
    async def test_two_missing_then_rejoin_separately(self):
        self.t=S; self.s.members['SIM'].pop('p1'); self.s.members['SIM'].pop('p2'); await self.s._sync_pauses()
        self.t=20*S; self.s.members['SIM']['p1']='new1'; await self.s._sync_pauses(); self.assertTrue(self.clock().paused)
        self.t=30*S; self.s.members['SIM']['p2']='new2'; await self.s._sync_pauses(); self.assertFalse(self.clock().paused)
        self.assertEqual(self.clock().snapshot(self.t)['white_ms'],119000)
    async def test_replay_keeps_used_budget(self):
        self.t=S; self.s.members['SIM'].pop('p1'); await self.s._sync_pauses()
        self.t=61*S; self.s.members['SIM']['p1']='new1'; await self.s._sync_pauses()
        self.room.record_verified_draw('R1M1'); self.games.clocks.sync(self.room,self.t)
        self.s.members['SIM'].pop('p1'); await self.s._sync_pauses()
        self.t=121*S; await self.s._sync_pauses()
        self.assertFalse(self.clock().paused); self.assertEqual(self.s.budgets['SIM','R1M1'],0)
    async def test_offline_player_in_new_round(self):
        self.t=5*S; self.room.record_verified_result('R1M1','p1','RESIGNATION'); self.games.clocks.sync(self.room,self.t)
        self.s.members['SIM'].pop('p1'); await self.s._sync_pauses()
        self.t=10*S; self.room.record_verified_result('R1M2','p3','RESIGNATION'); self.games.clocks.sync(self.room,self.t)
        await self.s._sync_pauses(); self.assertTrue(self.clock('R2M1').paused)
        self.assertEqual(self.clock('R2M1').snapshot(30*S)['white_ms'],120000)

if __name__=='__main__': unittest.main(verbosity=2)
