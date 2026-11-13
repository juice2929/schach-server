"""No network or production server: tests for server-verified chess games."""
import unittest
import chess
from online_tournament_core import TournamentRoom
from online_tournament_game_dev import VerifiedTournamentGames


class VerifiedGameTests(unittest.TestCase):
    def setUp(self):
        self.room = TournamentRoom('TEST1', 4, 'id1', 'Alice')
        for i, name in enumerate(('Bob', 'Carla', 'David'), 2):
            self.room.register('id' + str(i), name)
        self.games = VerifiedTournamentGames(self.room)

    def start(self):
        self.room.start('id1')

    def test_cannot_play_before_tournament_start(self):
        with self.assertRaises(ValueError):
            self.games.move('R1M1', 'id1', 'e2e4')

    def test_only_own_pair_can_play_and_read_state(self):
        self.start()
        with self.assertRaises(PermissionError):
            self.games.move('R1M1', 'id3', 'e2e4')
        with self.assertRaises(PermissionError):
            self.games.state('R1M1', 'id4')

    def test_legal_turns_and_public_state(self):
        self.start()
        first = self.games.move('R1M1', 'id1', 'e2e4')
        self.assertEqual(first['event'], 'move_accepted')
        self.assertEqual(first['state']['moves'], ['e2e4'])
        self.assertEqual(first['state']['turn'], 'black')
        self.assertEqual(first['state']['white'], 'Alice')
        self.assertNotIn('id1', str(first))
        second = self.games.move('R1M1', 'id2', 'e7e5')
        self.assertEqual(second['state']['moves'], ['e2e4', 'e7e5'])

    def test_reject_out_of_turn_and_keep_board(self):
        self.start()
        with self.assertRaises(PermissionError):
            self.games.move('R1M1', 'id2', 'e7e5')
        self.assertEqual(self.games.state('R1M1', 'id1')['moves'], [])

    def test_reject_illegal_or_malformed_move_without_mutation(self):
        self.start()
        for uci in ('e2e5', 'e7e5', 'badmove', None, 'e2e4;evil'):
            with self.subTest(uci=uci):
                with self.assertRaises(ValueError):
                    self.games.move('R1M1', 'id1', uci)
        self.assertEqual(self.games.state('R1M1', 'id1')['moves'], [])

    def test_checkmate_advances_winner_to_next_round(self):
        self.start()
        for pid, uci in (('id1','f2f3'), ('id2','e7e5'),
                         ('id1','g2g4'), ('id2','d8h4')):
            last = self.games.move('R1M1', pid, uci)
        self.assertEqual(last['event'], 'game_finished')
        self.assertEqual(last['state']['winner'], 'Bob')
        self.assertEqual(last['state']['reason'], 'CHECKMATE')
        self.assertEqual(self.room._get_match('R2M1').white_id, 'id2')

    def test_resignation_is_verified_by_identity(self):
        self.start()
        result = self.games.resign('R1M1', 'id2')
        self.assertEqual(result['state']['winner'], 'Alice')
        self.assertEqual(result['state']['reason'], 'RESIGNATION')
        self.assertEqual(self.room._get_match('R2M1').white_id, 'id1')
        with self.assertRaises(ValueError):
            self.games.resign('R1M1', 'id2')

    def test_invalid_draw_claim_rejected(self):
        self.start()
        with self.assertRaises(ValueError):
            self.games.claim_draw('R1M1', 'id1')
        self.assertEqual(self.room._get_match('R1M1').draw_replays, 0)

    def test_threefold_draw_claim_replays_pairing(self):
        self.start()
        sequence = [('id1','g1f3'), ('id2','g8f6'),
                    ('id1','f3g1'), ('id2','f6g8')] * 2
        for pid, uci in sequence:
            self.games.move('R1M1', pid, uci)
        result = self.games.claim_draw('R1M1', 'id1')
        self.assertEqual(result['event'], 'draw_replay')
        self.assertEqual(self.room._get_match('R1M1').draw_replays, 1)
        self.assertIsNone(self.room._get_match('R1M1').winner_id)
        self.assertEqual(self.games.move('R1M1', 'id1', 'e2e4')['state']['moves'], ['e2e4'])

    def test_completed_match_rejects_further_moves(self):
        self.start()
        self.games.resign('R1M1', 'id2')
        with self.assertRaises(ValueError):
            self.games.move('R1M1', 'id1', 'e2e4')

    def test_separate_matches_have_separate_boards(self):
        self.start()
        self.games.move('R1M1', 'id1', 'e2e4')
        second = self.games.state('R1M2', 'id3')
        self.assertEqual(second['moves'], [])
        self.assertEqual(second['fen'], chess.Board().fen())


if __name__ == '__main__':
    unittest.main(verbosity=2)
