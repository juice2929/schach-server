import unittest
import chess
from online_tournament_timed_games_dev import TimedTournamentGames
from test_online_tournament_clock_dev import room, S

class TimedGamesTests(unittest.TestCase):
    def make(self, minutes=2, n=4):
        self.t=0; self.room=room(n); g=TimedTournamentGames(self.room,minutes,lambda: self.t); g.start_clocks(); return g

    def test_legal_move_switches(self):
        g=self.make(); self.t=3*S; state=g.move('R1M1','p1','e2e4')['state']; self.assertEqual(state['clock']['white_ms'],117000); self.assertEqual(state['clock']['active'],'black')

    def test_illegal_move_does_not_switch(self):
        g=self.make(); self.t=S
        with self.assertRaises(ValueError): g.move('R1M1','p1','e2e5')
        self.t=2*S; self.assertEqual(g.state('R1M1','p1')['clock']['white_ms'],118000); self.assertEqual(g.state('R1M1','p1')['clock']['active'],'white')

    def test_unauthorized(self):
        g=self.make(); self.t=120*S
        with self.assertRaises(PermissionError): g.move('R1M1','p3','e2e4')
        self.assertIsNone(self.room._get_match('R1M1').winner_id)

    def test_expiry_without_moves(self):
        g=self.make(); self.t=120*S; results=g.poll_expired(); self.assertEqual(len(results),2); self.assertEqual(self.room._get_match('R1M1').winner_id,'p2'); self.assertEqual(self.room._get_match('R1M1').result_reason,'TIMEOUT'); self.assertEqual(g.poll_expired(),[]); self.assertEqual(g.state('R2M1','p2')['clock']['white_ms'],120000)

    def test_late_move_rejected_by_timeout(self):
        g=self.make(); self.t=120*S; result=g.move('R1M1','p1','e2e4'); self.assertEqual(result['event'],'game_finished'); self.assertEqual(result['state']['moves'],[]); self.assertEqual(result['state']['reason'],'TIMEOUT')

    def test_resign_and_next_round(self):
        g=self.make(); self.t=3*S; result=g.resign('R1M1','p2'); self.assertIsNone(result['state']['clock']['active']); self.t=5*S; g.resign('R1M2','p4'); self.assertEqual(g.state('R2M1','p1')['clock']['white_ms'],120000)

    def test_claim_draw_reset(self):
        g=self.make(); seq=['g1f3','g8f6','f3g1','f6g8']*2
        for i,m in enumerate(seq):
            self.t+=S; g.move('R1M1','p1' if i%2==0 else 'p2',m)
        result=g.claim_draw('R1M1','p1'); self.assertEqual(result['event'],'draw_replay'); state=g.state('R1M1','p1'); self.assertEqual(state['moves'],[]); self.assertEqual(state['clock']['white_ms'],120000); self.assertEqual(g.state('R1M2','p3')['clock']['white_ms'],112000)

    def test_flag_with_bare_king_opponent_is_draw(self):
        g=self.make(); g.games['R1M1']=chess.Board('4k3/8/8/8/8/8/4R3/4K3 w - - 0 1'); self.t=120*S; result=g.move('R1M1','p1','e2e3'); self.assertEqual(result['event'],'draw_replay'); self.assertIsNone(self.room._get_match('R1M1').winner_id)

    def test_repeated_state_does_not_reset(self):
        g=self.make(); self.t=5*S; g.state('R1M1','p1'); self.t=7*S; self.assertEqual(g.state('R1M1','p2')['clock']['white_ms'],113000)

    def test_untimed_unchanged_protocol(self):
        g=self.make(0); self.t=100000*S; self.assertEqual(g.poll_expired(),[]); result=g.move('R1M1','p1','e2e4'); self.assertNotIn('clock',result['state']); self.assertEqual(result['event'],'move_accepted')

    def test_eight_players_timeout_progress(self):
        g=self.make(2,8); self.t=120*S; self.assertEqual(len(g.poll_expired()),4); self.t=240*S; self.assertEqual(len(g.poll_expired()),2); self.t=360*S; self.assertEqual(len(g.poll_expired()),1); self.assertEqual(self.room.champion_id,'p8')

if __name__=='__main__': unittest.main(verbosity=2)
