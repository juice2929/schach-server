"""Safe offline tests; no Render/GitHub/WebSocket access required."""
import unittest
from online_tournament_core import TournamentRoom


def make_room(capacity):
    room = TournamentRoom(f"TEST{capacity}", capacity, "id1", "P1")
    for i in range(2, capacity + 1):
        room.register(f"id{i}", f"P{i}")
    return room


class TournamentRoomTests(unittest.TestCase):
    def test_four_players_to_champion(self):
        room = make_room(4)
        room.start("id1")
        self.assertEqual([m.match_id for m in room.ready_matches()], ["R1M1", "R1M2"])
        room.record_verified_result("R1M1", "id1", "CHECKMATE")
        room.record_verified_result("R1M2", "id4", "RESIGNATION")
        self.assertEqual([(m.white_id, m.black_id) for m in room.ready_matches()], [("id1", "id4")])
        room.record_verified_result("R2M1", "id4", "TIMEOUT")
        self.assertEqual(room.snapshot()["champion"], "P4")
        self.assertEqual(room.ready_matches(), [])

    def test_eight_players_to_champion(self):
        room = make_room(8)
        room.start("id1")
        for expected_count in (4, 2, 1):
            available = room.ready_matches()
            self.assertEqual(len(available), expected_count)
            for match in available:
                room.record_verified_result(match.match_id, match.white_id, "CHECKMATE")
        self.assertEqual(room.snapshot()["champion"], "P1")

    def test_duplicate_id(self):
        room = TournamentRoom("TEST", 4, "one", "Alice")
        with self.assertRaises(ValueError):
            room.register("one", "Bob")

    def test_duplicate_name_case_insensitive(self):
        room = TournamentRoom("TEST", 4, "one", "Alice")
        with self.assertRaises(ValueError):
            room.register("two", "aLiCe")

    def test_capacity_limit(self):
        room = make_room(4)
        with self.assertRaises(ValueError):
            room.register("id5", "P5")

    def test_invalid_size(self):
        with self.assertRaises(ValueError):
            TournamentRoom("TEST", 6, "host", "Alice")

    def test_start_requires_full_lobby(self):
        room = TournamentRoom("TEST", 4, "one", "Alice")
        with self.assertRaises(ValueError):
            room.start("one")

    def test_only_host_can_start(self):
        room = make_room(4)
        with self.assertRaises(PermissionError):
            room.start("id2")

    def test_registration_closed_after_start(self):
        room = make_room(4)
        room.start("id1")
        with self.assertRaises(ValueError):
            room.register("id5", "P5")

    def test_invalid_winner_and_reason(self):
        room = make_room(4)
        room.start("id1")
        with self.assertRaises(ValueError):
            room.record_verified_result("R1M1", "id3", "CHECKMATE")
        with self.assertRaises(ValueError):
            room.record_verified_result("R1M1", "id1", "CLIENT_SAYS_SO")
        self.assertIsNone(room._get_match("R1M1").winner_id)

    def test_duplicate_result_rejected(self):
        room = make_room(4)
        room.start("id1")
        room.record_verified_result("R1M1", "id1", "CHECKMATE")
        with self.assertRaises(ValueError):
            room.record_verified_result("R1M1", "id2", "CHECKMATE")

    def test_draw_keeps_match_ready(self):
        room = make_room(4)
        room.start("id1")
        room.record_verified_draw("R1M1")
        self.assertEqual(room._get_match("R1M1").draw_replays, 1)
        self.assertEqual(room._get_match("R1M1").winner_id, None)
        self.assertEqual(len(room.ready_matches()), 2)

    def test_public_view_hides_identity(self):
        room = make_room(4)
        snap = room.snapshot()
        self.assertIn("P1", snap["participants"])
        self.assertNotIn("id1", str(snap))

    def test_repeat_start_rejected(self):
        room = make_room(4)
        room.start("id1")
        with self.assertRaises(ValueError):
            room.start("id1")

    def test_rooms_are_independent(self):
        a = make_room(4)
        b = make_room(8)
        a.start("id1")
        a.record_verified_result("R1M1", "id1", "CHECKMATE")
        self.assertEqual(b.snapshot()["state"], "waiting")
        self.assertEqual(len(b.snapshot()["rounds"]), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
