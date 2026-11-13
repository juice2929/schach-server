"""Isolated local-development prototype: server-side online tournament bracket.

Schachspiel Ultimate Online V1.3

This module performs no network or file I/O and does NOT change server.py.
It is not yet wired to WebSockets or Discord. All identities come from the
trusted server connection layer (to be implemented), never from player-supplied
JSON. Call record_verified_result() ONLY after the SERVER has independently
validated the result (legal move history, authenticated resignation, or clock).
It does not itself verify chess moves, persistence, or reconnections.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


@dataclass
class Match:
    match_id: str
    round_number: int
    board_number: int
    white_id: Optional[str] = None
    black_id: Optional[str] = None
    winner_id: Optional[str] = None
    draw_replays: int = 0
    result_reason: Optional[str] = None

    @property
    def ready(self) -> bool:
        return self.white_id is not None and self.black_id is not None and self.winner_id is None


class TournamentRoom:
    """One 4-/8-person knockout room; all state stays local to this instance."""

    def __init__(self, code: str, capacity: int, host_id: str, host_name: str):
        if capacity not in (4, 8):
            raise ValueError("Turniergröße muss 4 oder 8 sein.")
        if not isinstance(code, str) or not 3 <= len(code.strip()) <= 32:
            raise ValueError("Turniercode muss 3 bis 32 Zeichen enthalten.")
        self.code = code.strip()
        self.capacity = capacity
        self.players: dict[str, str] = {}  # Trustworthy connection identity -> visible name
        self.started = False
        self.champion_id: Optional[str] = None
        self.rounds: list[list[Match]] = []
        self.register(host_id, host_name)
        self.host_id = host_id

    def register(self, player_id: str, name: str) -> None:
        if self.started:
            raise ValueError("Turnier bereits gestartet.")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("Ungültige Server-Spielerkennung.")
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 40:
            raise ValueError("Name muss 1 bis 40 Zeichen haben.")
        player_id, name = player_id.strip(), name.strip()
        if player_id in self.players:
            raise ValueError("Spieler ist bereits angemeldet.")
        if len(self.players) >= self.capacity:
            raise ValueError("Turnier ist voll.")
        if name.casefold() in {n.casefold() for n in self.players.values()}:
            raise ValueError("Name ist bereits vergeben.")
        self.players[player_id] = name

    def start(self, acting_host_id: str) -> None:
        if acting_host_id != self.host_id:
            raise PermissionError("Nur die Turnierleitung darf starten.")
        if self.started:
            raise ValueError("Turnier wurde bereits gestartet.")
        if len(self.players) != self.capacity:
            raise ValueError(f"Noch nicht genügend Teilnehmer ({len(self.players)}/{self.capacity}).")

        ids = list(self.players)  # registration order
        rounds_count = 2 if self.capacity == 4 else 3
        self.rounds = []
        for r in range(rounds_count):
            matches = [Match(f"R{r + 1}M{i + 1}", r + 1, i + 1)
                       for i in range(self.capacity // (2 ** (r + 1)))]
            if r == 0:
                for i, match in enumerate(matches):
                    match.white_id = ids[2 * i]
                    match.black_id = ids[2 * i + 1]
            self.rounds.append(matches)
        self.started = True

    def _get_match(self, match_id: str) -> Match:
        for round_matches in self.rounds:
            for match in round_matches:
                if match.match_id == match_id:
                    return match
        raise ValueError("Unbekannte Turnierpartie.")

    def ready_matches(self) -> list[Match]:
        return [m for r in self.rounds for m in r if m.ready]

    def record_verified_draw(self, match_id: str) -> None:
        """SERVER-ONLY: call after the server independently verifies a drawn game."""
        if not self.started:
            raise ValueError("Turnier wurde noch nicht gestartet.")
        match = self._get_match(match_id)
        if not match.ready or self.champion_id is not None:
            raise ValueError("Partie ist nicht offen.")
        match.draw_replays += 1

    def record_verified_result(self, match_id: str, winner_id: str, reason: str) -> None:
        """SERVER-ONLY: result must already be authenticated/verified by server.

        DO NOT expose this method as a network 'report winner' action. It is a
        bracket transition, not chess-result validation.
        """
        if not self.started:
            raise ValueError("Turnier wurde noch nicht gestartet.")
        match = self._get_match(match_id)
        if not match.ready or self.champion_id is not None:
            raise ValueError("Partie ist nicht offen.")
        if winner_id not in (match.white_id, match.black_id):
            raise ValueError("Gewinner gehört nicht zu dieser Partie.")
        if reason not in ("CHECKMATE", "RESIGNATION", "TIMEOUT", "FORFEIT"):
            raise ValueError("Ungültiger Ergebnisgrund.")
        match.winner_id = winner_id
        match.result_reason = reason
        r = match.round_number - 1
        if r == len(self.rounds) - 1:
            self.champion_id = winner_id
            return
        next_match = self.rounds[r + 1][(match.board_number - 1) // 2]
        next_slot = "white_id" if (match.board_number - 1) % 2 == 0 else "black_id"
        if getattr(next_match, next_slot) is not None:
            raise RuntimeError("Folgerunden-Platz ist bereits belegt.")
        setattr(next_match, next_slot, winner_id)

    def snapshot(self) -> dict:
        """Public UI view: never leaks internal connection/player identifiers."""
        def display(player_id):
            return self.players.get(player_id) if player_id is not None else None

        return {
            "code": self.code,
            "capacity": self.capacity,
            "state": ("waiting" if not self.started else
                      "finished" if self.champion_id is not None else "running"),
            "participants": list(self.players.values()),
            "champion": display(self.champion_id),
            "rounds": [[{
                "match_id": m.match_id,
                "white": display(m.white_id),
                "black": display(m.black_id),
                "winner": display(m.winner_id),
                "draw_replays": m.draw_replays,
                "result_reason": m.result_reason,
            } for m in matches] for matches in self.rounds],
        }
