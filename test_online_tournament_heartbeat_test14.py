
import asyncio
import unittest
from unittest.mock import patch

from test_online_tournament_lobby_resume_test14 import MockSocket
from online_tournament_ws_clock_dev import TimedTournamentWebSocketService


class LobbyHeartbeatTests(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.svc = TimedTournamentWebSocketService()
        self.code = "HEARTBEAT"
        self.time = 1000.0

        for module in (
            "online_tournament_resume_dev",
            "online_tournament_ws_clock_dev"
        ):
            timer = patch(
                module + ".monotonic",
                side_effect=lambda: self.time
            )
            timer.start()
            self.addCleanup(timer.stop)

    async def register(self, name, host=False, heartbeat=True):
        ws = MockSocket()

        data = {
            "action": "tournament_create" if host else "tournament_join",
            "code": self.code,
            "name": name
        }

        if heartbeat:
            data.update(lobby_resume=True, lobby_heartbeat=True)

        if host:
            data.update(capacity=4, time_minutes=5)

        await self.svc.process(ws, data)

        reply = ws.packet(
            "tournament_created" if host else "tournament_joined"
        )

        token = ws.packet("tournament_resume_credentials")["token"]
        return ws, reply["player_id"], token

    async def resume(self, pid, token):
        ws = MockSocket()

        await self.svc.process(ws, {
            "action": "tournament_resume",
            "code": self.code,
            "player_id": pid,
            "token": token,
            "replace_connection": True,
            "lobby_heartbeat": True
        })

        return ws

    async def advance(self, seconds):
        self.time += seconds
        await self.svc.poll_once()

        await asyncio.gather(
            *tuple(self.svc._replaced_socket_tasks)
        )

    async def test_regular_heartbeat_keeps_lobby(self):
        host, pid, _ = await self.register("Host", host=True)

        for _ in range(60):
            await self.advance(10)
            await self.svc.process(host, {
                "action": "tournament_heartbeat"
            })

        self.assertIn(self.code, self.svc.rooms)
        self.assertIs(self.svc.members[self.code][pid], host)
        self.assertFalse(host.closed)

    async def test_exact_heartbeat_timeout(self):
        host, pid, _ = await self.register("Host", host=True)

        await self.advance(29)
        self.assertFalse(host.closed)

        await self.advance(1)
        self.assertTrue(host.closed)

        self.assertNotIn(host, self.svc.membership)
        self.assertIn(self.code, self.svc.rooms)

        self.assertEqual(
            self.svc._lobby_deadlines[(self.code, pid)],
            self.time + 240
        )

    async def test_resume_within_grace_period(self):
        old, pid, token = await self.register("Host", host=True)

        await self.advance(30)
        await self.advance(239)

        new = await self.resume(pid, token)

        self.assertEqual(
            new.packet("tournament_resumed")["snapshot"]["state"],
            "waiting"
        )

        self.assertNotIn((self.code, pid), self.svc._lobby_deadlines)
        self.assertIn(new, self.svc._lobby_heartbeats)

        await self.svc.disconnect(old)

        self.assertIs(self.svc.members[self.code][pid], new)

    async def test_resume_after_grace_period_rejected(self):
        host, pid, token = await self.register("Host", host=True)

        guest, _, _ = await self.register(
            "WindowsGuest", heartbeat=False
        )

        await self.advance(30)
        await self.advance(239)

        self.assertIn(self.code, self.svc.rooms)

        await self.advance(1)

        self.assertNotIn(self.code, self.svc.rooms)
        self.assertNotIn(self.code, self.svc.minutes)
        self.assertNotIn((self.code, pid), self.svc._resume_proofs)

        guest.packet("tournament_cancelled")

        with self.assertRaises(PermissionError):
            await self.resume(pid, token)

    async def test_guest_expiry_keeps_host(self):
        host, _, _ = await self.register(
            "WindowsHost", host=True, heartbeat=False
        )

        guest, pid, _ = await self.register("AndroidGuest")

        await self.advance(30)
        await self.advance(240)

        self.assertTrue(guest.closed)
        self.assertFalse(host.closed)
        self.assertIn(self.code, self.svc.rooms)
        self.assertNotIn(pid, self.svc.rooms[self.code].players)
        self.assertEqual(len(self.svc.rooms[self.code].players), 1)

    async def test_legacy_windows_unchanged(self):
        host, pid, _ = await self.register(
            "WindowsHost", host=True, heartbeat=False
        )

        await self.advance(600)

        self.assertNotIn(host, self.svc._lobby_heartbeats)
        self.assertIs(self.svc.members[self.code][pid], host)
        self.assertFalse(host.closed)

    async def test_started_tournament_unchanged(self):
        host, pid, _ = await self.register("Host", host=True)

        for name in ("Guest1", "Guest2", "Guest3"):
            await self.register(name)

        await self.svc.process(host, {
            "action": "tournament_start",
            "code": self.code
        })

        games = self.svc.verified_games[self.code]

        await self.advance(600)

        self.assertTrue(self.svc.rooms[self.code].started)
        self.assertFalse(host.closed)
        self.assertIs(self.svc.members[self.code][pid], host)
        self.assertIs(self.svc.verified_games[self.code], games)
        self.assertFalse(self.svc._lobby_heartbeats)

    async def test_explicit_leave_cleans_heartbeat(self):
        host, _, _ = await self.register("Host", host=True)
        guest, _, _ = await self.register("Guest")

        await self.svc.process(host, {
            "action": "tournament_leave",
            "code": self.code
        })

        self.assertNotIn(self.code, self.svc.rooms)
        self.assertNotIn(host, self.svc._lobby_heartbeats)
        self.assertNotIn(guest, self.svc._lobby_heartbeats)



    async def test_heartbeat_negotiation(self):
        host, _, _ = await self.register("AndroidHost", host=True)

        legacy, _, _ = await self.register(
            "WindowsGuest", heartbeat=False
        )

        self.assertIs(
            host.packet("tournament_resume_credentials").get(
                "lobby_heartbeat"
            ), True
        )

        self.assertNotIn(
            "lobby_heartbeat",
            legacy.packet("tournament_resume_credentials")
        )

    async def test_heartbeat_ack_is_private(self):
        host, _, _ = await self.register("Host", host=True)
        guest, _, _ = await self.register("Guest")

        before = len(guest.messages)

        self.time += 10

        await self.svc.process(host, {
            "action": "tournament_heartbeat"
        })

        host.packet("tournament_heartbeat_ack")

        self.assertEqual(len(guest.messages), before)
        self.assertEqual(
            self.svc._lobby_heartbeats[host], self.time
        )

    async def test_heartbeat_without_opt_in_is_ignored(self):
        host, _, _ = await self.register("Host", host=True)

        legacy, _, _ = await self.register(
            "WindowsGuest", heartbeat=False
        )

        outsider = MockSocket()

        before = self.svc._lobby_heartbeats[host]

        for ws in (legacy, outsider):
            await self.svc.process(ws, {
                "action": "tournament_heartbeat"
            })

            self.assertNotIn(ws, self.svc._lobby_heartbeats)

            self.assertFalse(any(
                p.get("action") == "tournament_heartbeat_ack"
                for p in ws.messages
            ))

        self.assertEqual(
            self.svc._lobby_heartbeats[host], before
        )

    async def test_resume_renegotiates_heartbeat(self):
        old, pid, token = await self.register("Host", host=True)

        await self.advance(30)

        new = await self.resume(pid, token)

        self.assertIs(
            new.packet("tournament_resumed").get(
                "lobby_heartbeat"
            ), True
        )

        self.assertNotIn(old, self.svc._lobby_heartbeats)

        await self.svc.process(new, {
            "action": "tournament_heartbeat"
        })

        new.packet("tournament_heartbeat_ack")

    async def test_running_game_has_no_lobby_heartbeat(self):
        host, pid, token = await self.register("Host", host=True)

        for name in ("Guest1", "Guest2", "Guest3"):
            await self.register(name)

        await self.svc.process(host, {
            "action": "tournament_start",
            "code": self.code
        })

        await self.svc.poll_once()

        self.assertFalse(self.svc._lobby_heartbeats)

        await self.svc.disconnect(host)

        new = await self.resume(pid, token)

        self.assertNotIn(
            "lobby_heartbeat",
            new.packet("tournament_resumed")
        )

        await self.svc.process(new, {
            "action": "tournament_heartbeat"
        })

        self.assertFalse(any(
            p.get("action") == "tournament_heartbeat_ack"
            for p in new.messages
        ))


if __name__ == "__main__":
    unittest.main(verbosity=2)
