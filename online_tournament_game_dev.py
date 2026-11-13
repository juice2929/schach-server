"""Server-authoritative CHESS RULES for local tournament development (V1.3).

This deliberately does not bind sockets, launch a server, or modify server.py.
It must only be called with actor_id taken from the WebSocket server's
connection membership (never from untrusted JSON). A later isolated adapter
will integrate it with the WebSocket lobby under one asyncio.Lock.

Uses python-chess already used by the Windows chess client. No timers, durable
storage, reconnects, or draw offers in this prototype. Clock TIMEOUT is not
accepted here: it requires server-owned monotonic timers in a later phase.
"""

from __future__ import annotations

import chess
from online_tournament_core import TournamentRoom


class VerifiedTournamentGames:
    """A single room's games. Caller serializes changes when used over WebSockets."""

    def __init__(self, room: TournamentRoom):
        if not isinstance(room, TournamentRoom):
            raise TypeError('TournamentRoom erforderlich.')
        self.room = room
        self.games: dict[str, chess.Board] = {}

    def _authorized_match(self, match_id: str, actor_id: str):
        if not self.room.started:
            raise ValueError('Turnier ist noch nicht gestartet.')
        match = self.room._get_match(match_id)
        if actor_id not in (match.white_id, match.black_id):
            raise PermissionError('Du spielst nicht in dieser Partie.')
        return match

    def _board(self, match):
        board = self.games.get(match.match_id)
        if board is None:
            if not match.ready:
                raise ValueError('Partie ist nicht offen.')
            board = chess.Board()
            self.games[match.match_id] = board
        return board

    def _state(self, match, board):
        # Only names, public board state and chess notation; never client IDs.
        return {
            'match_id': match.match_id,
            'white': self.room.players[match.white_id],
            'black': self.room.players[match.black_id],
            'fen': board.fen(),
            'turn': 'white' if board.turn == chess.WHITE else 'black',
            'moves': [move.uci() for move in board.move_stack],
            'winner': (self.room.players[match.winner_id] if match.winner_id else None),
            'reason': match.result_reason,
            'draw_replays': match.draw_replays,
        }

    def state(self, match_id: str, actor_id: str) -> dict:
        """State is private to the two assigned players for now."""
        match = self._authorized_match(match_id, actor_id)
        return self._state(match, self._board(match))

    def move(self, match_id: str, actor_id: str, move_uci: str) -> dict:
        """Apply one fully legal move, then independently adjudicate game end."""
        match = self._authorized_match(match_id, actor_id)
        if not match.ready:
            raise ValueError('Partie ist bereits beendet.')
        board = self._board(match)
        expected = match.white_id if board.turn == chess.WHITE else match.black_id
        if actor_id != expected:
            raise PermissionError('Du bist nicht am Zug.')
        if not isinstance(move_uci, str) or len(move_uci) not in (4, 5):
            raise ValueError('Ungültiges UCI-Zugformat.')
        try:
            move = chess.Move.from_uci(move_uci)
        except ValueError as exc:
            raise ValueError('Ungültiges UCI-Zugformat.') from exc
        if move not in board.legal_moves:
            raise ValueError('Unzulässiger Schachzug.')
        board.push(move)
        outcome = board.outcome(claim_draw=False)
        event = 'move_accepted'
        if outcome is not None:
            if outcome.winner is None:
                self.room.record_verified_draw(match_id)
                del self.games[match_id]
                event = 'draw_replay'
            else:
                winner_id = match.white_id if outcome.winner == chess.WHITE else match.black_id
                self.room.record_verified_result(match_id, winner_id, 'CHECKMATE'
                                                 if board.is_checkmate() else 'FORFEIT')
                event = 'game_finished'
        return {'event': event, 'state': self._state(match, board)}

    def resign(self, match_id: str, actor_id: str) -> dict:
        """Only the actual participant's authenticated resignation is accepted."""
        match = self._authorized_match(match_id, actor_id)
        if not match.ready:
            raise ValueError('Partie ist bereits beendet.')
        board = self._board(match)
        winner_id = match.black_id if actor_id == match.white_id else match.white_id
        self.room.record_verified_result(match_id, winner_id, 'RESIGNATION')
        return {'event': 'game_finished', 'state': self._state(match, board)}

    def claim_draw(self, match_id: str, actor_id: str) -> dict:
        """Allow a legal 3-fold/50-move claim by the side to move."""
        match = self._authorized_match(match_id, actor_id)
        if not match.ready:
            raise ValueError('Partie ist bereits beendet.')
        board = self._board(match)
        expected = match.white_id if board.turn == chess.WHITE else match.black_id
        if actor_id != expected:
            raise PermissionError('Du bist nicht am Zug.')
        if not board.can_claim_draw():
            raise ValueError('Remis kann hier nicht beansprucht werden.')
        self.room.record_verified_draw(match_id)
        del self.games[match_id]
        return {'event': 'draw_replay', 'state': self._state(match, board)}
