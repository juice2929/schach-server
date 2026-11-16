import unittest
from online_tournament_clock_dev import MatchClock, TournamentClockBook, validate_minutes
from online_tournament_core import TournamentRoom

S = 1_000_000_000


def room(n=4):
    r = TournamentRoom('CLOCK', n, 'p1', 'P1')
    for i in range(2, n+1):
        r.register(f'p{i}', f'P{i}')
    r.start('p1')
    return r


class ClockTests(unittest.TestCase):
    def test_options(self):
        for n in (0,2,3,5,10,15,30):
            self.assertEqual(validate_minutes(n), n)
        for n in (-1,1,4,60,True,2.0,'5',None):
            with self.assertRaises(ValueError): validate_minutes(n)

    def test_white_starts(self):
        self.assertEqual(MatchClock(5,0).snapshot(0), {'minutes':5,'white_ms':300000,'black_ms':300000,'active':'white','expired':None})

    def test_only_active_decreases(self):
        c=MatchClock(2,0); self.assertEqual(c.snapshot(3*S)['white_ms'],117000); self.assertEqual(c.snapshot(3*S)['black_ms'],120000)

    def test_switch(self):
        c=MatchClock(2,0); c.accept_move('white',3*S); c.accept_move('black',8*S)
        self.assertEqual(c.snapshot(10*S)['white_ms'],115000); self.assertEqual(c.snapshot(10*S)['black_ms'],115000)

    def test_wrong_side(self):
        c=MatchClock(2,0)
        with self.assertRaises(ValueError): c.accept_move('black',2*S)
        self.assertEqual(c.snapshot(3*S)['active'],'white')

    def test_flag_exact_boundary(self):
        c=MatchClock(2,0); self.assertIsNone(c.expired_side(120*S-1)); self.assertEqual(c.expired_side(120*S),'white')
        with self.assertRaises(ValueError): c.accept_move('white',120*S)

    def test_black_timeout(self):
        c=MatchClock(2,0); c.accept_move('white',1*S); self.assertEqual(c.expired_side(121*S),'black')

    def test_stop(self):
        c=MatchClock(2,0); c.stop(3*S); c.stop(4*S); self.assertEqual(c.snapshot(500*S)['white_ms'],117000); self.assertIsNone(c.expired_side(500*S))

    def test_expired_stop(self):
        c=MatchClock(2,0); c.stop(121*S); self.assertEqual(c.snapshot(500*S)['expired'],'white')

    def test_no_time(self):
        c=MatchClock(0,0); c.accept_move('white',1000*S); self.assertIsNone(c.snapshot(2000*S)); self.assertIsNone(c.expired_side(2000*S))

    def test_monotonic_guard(self):
        with self.assertRaises(ValueError): MatchClock(2,10).snapshot(9)

    def test_snapshot_does_not_reset(self):
        c=MatchClock(2,0); c.snapshot(S); self.assertEqual(c.snapshot(2*S)['white_ms'],118000)

    def test_four_parallel_clocks(self):
        r=room(8); b=TournamentClockBook(2); b.sync(r,0); b.get('R1M1').accept_move('white',S)
        self.assertEqual(b.get('R1M1').snapshot(3*S)['active'],'black')
        for i in (2,3,4): self.assertEqual(b.get(f'R1M{i}').snapshot(3*S)['white_ms'],117000)

    def test_replay_new_clock_only_for_one_match(self):
        r=room(); b=TournamentClockBook(3); b.sync(r,0); old=b.get('R1M1'); r.record_verified_draw('R1M1'); b.sync(r,5*S)
        self.assertIsNot(old,b.get('R1M1')); self.assertEqual(b.get('R1M1').snapshot(5*S)['white_ms'],180000)
        self.assertEqual(b.get('R1M2').snapshot(5*S)['white_ms'],175000)

    def test_next_round_starts_when_ready(self):
        r=room(); b=TournamentClockBook(5); b.sync(r,0)
        r.record_verified_result('R1M1','p1','RESIGNATION'); b.sync(r,S); self.assertIsNone(b.get('R2M1'))
        r.record_verified_result('R1M2','p3','RESIGNATION'); b.sync(r,2*S); self.assertEqual(b.get('R2M1').snapshot(2*S)['white_ms'],300000)
        self.assertIsNone(b.get('R1M1').snapshot(10*S)['active']); self.assertEqual(b.get('R1M1').snapshot(10*S)['white_ms'],299000)

    def test_sync_and_reconnect_do_not_restart(self):
        r=room(); b=TournamentClockBook(5); b.sync(r,0); c=b.get('R1M1'); b.sync(r,10*S)
        self.assertIs(c,b.get('R1M1')); self.assertEqual(c.snapshot(10*S)['white_ms'],290000)

    def test_untimed_book(self):
        b=TournamentClockBook(0); b.sync(room(),0); self.assertIsNone(b.get('R1M1'))

if __name__=='__main__': unittest.main(verbosity=2)
