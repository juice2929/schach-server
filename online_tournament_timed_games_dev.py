"""Verified game layer plus authoritative clocks; not yet wired to WebSockets.
Call start_clocks immediately after room.start, and poll_expired periodically
under the same service lock as moves. Broadcast each returned outcome and
updated bracket before accepting later events. No production server changes.
"""
from online_tournament_clock_dev import TournamentClockBook
from online_tournament_game_dev import VerifiedTournamentGames
from time import monotonic_ns
import chess


class TimedTournamentGames(VerifiedTournamentGames):
    def __init__(self, room, minutes=0, now=monotonic_ns):
        super().__init__(room)
        self.clocks = TournamentClockBook(minutes, now)

    def start_clocks(self):
        if not self.room.started:
            raise ValueError('Turnier ist noch nicht gestartet.')
        self.clocks.sync(self.room)

    def _clock(self, match_id):
        clock = self.clocks.get(match_id)
        if self.clocks.minutes and clock is None:
            raise ValueError('Schachuhr wurde noch nicht gestartet.')
        return clock

    def state(self, match_id, actor_id):
        state = super().state(match_id, actor_id)
        clock = self._clock(match_id)
        if clock:
            state['clock'] = clock.snapshot(self.clocks.now())
        return state

    def _flag(self, match, clock, now):
        if not match.ready or clock is None:
            return None
        expired = clock.expired_side(now)
        if expired is None:
            return None
        board = self._board(match)
        winner_color = chess.BLACK if expired == 'white' else chess.WHITE
        clock.stop(now)
        # python-chess recognizes insufficient mating material. This is not
        # an exhaustive search for every possible FIDE dead position.
        if board.has_insufficient_material(winner_color):
            self.room.record_verified_draw(match.match_id)
            del self.games[match.match_id]
            event = 'draw_replay'
        else:
            winner_id = match.white_id if winner_color else match.black_id
            self.room.record_verified_result(match.match_id, winner_id, 'TIMEOUT')
            event = 'game_finished'
        state = self._state(match, board)
        state['clock'] = clock.snapshot(now)
        self.clocks.sync(self.room, now)
        return {'event': event, 'state': state, 'cause': 'TIMEOUT'}

    def poll_expired(self):
        """Return timeout results even when neither player sends any move."""
        now = self.clocks.now()
        outcomes = []
        for match in self.room.ready_matches():
            outcome = self._flag(match, self._clock(match.match_id), now)
            if outcome is not None:
                outcomes.append(outcome)
        return outcomes

    def _action(self, action, match_id, actor_id, *args):
        match = self._authorized_match(match_id, actor_id)
        if not match.ready:
            raise ValueError('Partie ist bereits beendet.')
        clock = self._clock(match_id)
        now = self.clocks.now()
        if clock is not None and clock.paused and action != 'resign':
            raise ValueError('Schachuhr ist pausiert.')
        if clock is not None:
            outcome = self._flag(match, clock, now)
            if outcome is not None:
                return outcome
        board = self._board(match)
        side = 'white' if board.turn else 'black'
        # Parent rejects illegal moves/claims before the clock changes.
        outcome = getattr(super(), action)(match_id, actor_id, *args)
        if clock is not None:
            if outcome['event'] == 'move_accepted':
                clock.accept_move(side, now)
            else:
                clock.stop(now)
            outcome['state']['clock'] = clock.snapshot(now)
            self.clocks.sync(self.room, now)
        return outcome

    def move(self, match_id, actor_id, move_uci):
        return self._action('move', match_id, actor_id, move_uci)

    def resign(self, match_id, actor_id):
        return self._action('resign', match_id, actor_id)

    def claim_draw(self, match_id, actor_id):
        return self._action('claim_draw', match_id, actor_id)
