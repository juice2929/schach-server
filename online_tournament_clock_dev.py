"""Server-owned, optional chess clocks. Pure Python; not a network server.
Caller must serialize all game/clock actions under the tournament service lock.
No client clocks, wall-clock dates, or network timestamps are trusted.
"""
from dataclasses import dataclass
from time import monotonic_ns

TIME_OPTIONS = (0, 2, 3, 5, 10, 15, 30)
NS_PER_MS = 1_000_000
NS_PER_MINUTE = 60_000_000_000


def validate_minutes(minutes):
    if type(minutes) is not int or minutes not in TIME_OPTIONS:
        raise ValueError('Zeit muss 0, 2, 3, 5, 10, 15 oder 30 Minuten sein.')
    return minutes


@dataclass
class MatchClock:
    minutes: int
    started_ns: int

    def __post_init__(self):
        validate_minutes(self.minutes)
        self._remaining = [self.minutes * NS_PER_MINUTE] * 2
        self._active = 0 if self.minutes else None  # white=0, black=1
        self._last_ns = self.started_ns
        self._expired = None
        self._paused_side = None
        self._stopped = not bool(self.minutes)

    def _elapsed(self, now):
        if type(now) is not int or now < self._last_ns:
            raise ValueError('Monotone Serverzeit erforderlich.')
        return now - self._last_ns

    def _view(self, now):
        elapsed = self._elapsed(now)
        remaining = self._remaining.copy()
        if self._active is not None:
            remaining[self._active] = max(0, remaining[self._active] - elapsed)
        return remaining

    def expired_side(self, now):
        remaining = self._view(now)
        if self._expired is not None:
            return self._expired
        if self._active is not None and remaining[self._active] == 0:
            return ('white', 'black')[self._active]
        return None

    def snapshot(self, now):
        if not self.minutes:
            return None
        remaining = self._view(now)
        expired = self.expired_side(now)
        return {'minutes': self.minutes,
                'white_ms': (remaining[0] + NS_PER_MS - 1) // NS_PER_MS,
                'black_ms': (remaining[1] + NS_PER_MS - 1) // NS_PER_MS,
                'active': None if expired or self._active is None else ('white', 'black')[self._active],
                'expired': expired}

    @property
    def paused(self):
        return self._paused_side is not None

    def pause(self, now):
        """Freeze at server detection time, without restoring spent time."""
        if self._stopped or self.paused or self.expired_side(now) is not None:
            return False
        self._remaining = self._view(now)
        self._last_ns = now
        self._paused_side = self._active
        self._active = None
        return True

    def resume(self, now):
        if not self.paused or self._stopped:
            return False
        self._elapsed(now)
        self._last_ns = now
        self._active = self._paused_side
        self._paused_side = None
        return True

    def accept_move(self, side, now):
        """Call only AFTER chess legality is verified, using the server receipt time."""
        if side not in ('white', 'black'):
            raise ValueError('Ungültige Zugfarbe.')
        if not self.minutes:
            return
        if self._stopped or self.paused or self.expired_side(now) is not None:
            raise ValueError('Schachuhr ist abgelaufen oder beendet.')
        if side != ('white', 'black')[self._active]:
            raise ValueError('Falsche Zugfarbe für die Schachuhr.')
        self._remaining = self._view(now)
        self._last_ns = now
        self._active = 1 - self._active

    def stop(self, now):
        """Freeze once; retain flag-fall evidence in final snapshot."""
        if self._stopped:
            return
        self._expired = self.expired_side(now)
        self._remaining = self._view(now)
        self._last_ns = now
        self._active = None
        self._paused_side = None
        self._stopped = True


class TournamentClockBook:
    """One independent clock per ready match and replay generation."""
    def __init__(self, minutes, now=monotonic_ns):
        self.minutes = validate_minutes(minutes)
        self.now = now
        self._clocks = {}  # match_id -> (draw_replays, MatchClock)

    def sync(self, room, now=None):
        """Run on initial start, result and draw replay; start newly-ready games."""
        now = self.now() if now is None else now
        if not self.minutes:
            return
        ready = {m.match_id: m for m in room.ready_matches()}
        for match_id, (_, clock) in self._clocks.items():
            if match_id not in ready:
                clock.stop(now)
        for match_id, match in ready.items():
            previous = self._clocks.get(match_id)
            if previous is None or previous[0] != match.draw_replays:
                self._clocks[match_id] = (match.draw_replays, MatchClock(self.minutes, now))

    def get(self, match_id):
        entry = self._clocks.get(match_id)
        return entry[1] if entry else None
