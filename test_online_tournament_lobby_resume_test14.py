import asyncio
import json
import unittest

from online_tournament_ws_clock_dev import TimedTournamentWebSocketService


class MockSocket:
    def __init__(self):
        self.messages = []
        self.closed = False

    async def send(self, raw):
        self.messages.append(json.loads(raw))

    async def close(self, code=1000, reason=''):
        self.closed = True

    def packet(self, action):
        return next(
            p for p in reversed(self.messages)
            if p.get("action") == action
        )


class LobbyReconnectTests(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.svc = TimedTournamentWebSocketService()
        self.code = "TEST14-LOBBY"

    async def register(self, name, host=False, mobile=True):
        ws = MockSocket()
        data = {
            "action": "tournament_create" if host else "tournament_join",
            "code": self.code,
            "name": name
        }
        if mobile:
            data["lobby_resume"] = True
        if host:
            data.update(capacity=4, time_minutes=5)

        await self.svc.process(ws, data)

        reply = ws.packet(
            "tournament_created" if host else "tournament_joined"
        )
        token = ws.packet("tournament_resume_credentials")["token"]
        return ws, reply["player_id"], token

    async def resume(self, player_id, token):
        ws = MockSocket()
        await self.svc.process(ws, {
            "action": "tournament_resume",
            "code": self.code,
            "player_id": player_id,
            "token": token
        })
        return ws

    async def test_android_host_resume(self):
        host, pid, token = await self.register("Host", host=True)
        await self.svc.disconnect(host)

        self.assertIn(self.code, self.svc.rooms)

        recovered = await self.resume(pid, token)
        reply = recovered.packet("tournament_resumed")

        self.assertEqual(reply["snapshot"]["state"], "waiting")
        self.assertEqual(reply["snapshot"]["time_minutes"], 5)
        self.assertEqual(reply["game_states"], [])
        self.assertEqual(self.svc.rooms[self.code].host_id, pid)
        self.assertEqual(len(self.svc.rooms[self.code].players), 1)

    async def test_android_guest_resume(self):
        await self.register("Host", host=True)
        guest, pid, token = await self.register("Guest")

        await self.svc.disconnect(guest)

        self.assertIn(pid, self.svc.rooms[self.code].players)

        recovered = await self.resume(pid, token)
        reply = recovered.packet("tournament_resumed")

        self.assertEqual(reply["snapshot"]["state"], "waiting")
        self.assertEqual(len(self.svc.rooms[self.code].players), 2)
        self.assertEqual(reply["game_states"], [])

    async def test_legacy_windows_host_disconnect(self):
        host, _, _ = await self.register(
            "WindowsHost", host=True, mobile=False
        )
        await self.svc.disconnect(host)

        self.assertNotIn(self.code, self.svc.rooms)



    async def test_guest_reservation_expires(self):
        await self.register("Host", host=True)
        guest, pid, token = await self.register("Guest")

        await self.svc.disconnect(guest)

        await self.svc.expire_lobby_reservations(now=0)
        self.assertIn(pid, self.svc.rooms[self.code].players)

        await self.svc.expire_lobby_reservations(now=float("inf"))
        self.assertNotIn(pid, self.svc.rooms[self.code].players)
        self.assertNotIn((self.code, pid), self.svc._resume_proofs)

        with self.assertRaises(PermissionError):
            await self.resume(pid, token)

    async def test_host_reservation_expires(self):
        host, pid, token = await self.register("Host", host=True)
        await self.register("Guest")

        await self.svc.disconnect(host)
        await self.svc.expire_lobby_reservations(now=float("inf"))

        self.assertNotIn(self.code, self.svc.rooms)
        self.assertNotIn(self.code, self.svc.members)
        self.assertNotIn(self.code, self.svc.minutes)
        self.assertFalse(any(
            key[0] == self.code for key in self.svc._resume_proofs
        ))

    async def test_explicit_guest_leave(self):
        await self.register("Host", host=True)
        guest, pid, token = await self.register("Guest")

        await self.svc.process(guest, {
            "action": "tournament_leave",
            "code": self.code
        })

        self.assertNotIn(pid, self.svc.rooms[self.code].players)
        self.assertNotIn((self.code, pid), self.svc._resume_proofs)

    async def test_explicit_host_leave(self):
        host, _, _ = await self.register("Host", host=True)
        await self.register("Guest")

        await self.svc.process(host, {
            "action": "tournament_leave",
            "code": self.code
        })

        self.assertNotIn(self.code, self.svc.rooms)
        self.assertNotIn(self.code, self.svc.minutes)



    async def replace_connection(self, pid, token):
        ws = MockSocket()
        await self.svc.process(ws, {
            "action": "tournament_resume",
            "code": self.code,
            "player_id": pid,
            "token": token,
            "replace_connection": True
        })

        await asyncio.gather(
            *tuple(self.svc._replaced_socket_tasks)
        )
        return ws

    async def test_replace_active_host(self):
        old, pid, token = await self.register("Host", host=True)

        new = await self.replace_connection(pid, token)

        self.assertTrue(old.closed)
        self.assertNotIn(old, self.svc.membership)
        self.assertIs(self.svc.members[self.code][pid], new)
        self.assertEqual(self.svc.rooms[self.code].host_id, pid)
        self.assertEqual(len(self.svc.rooms[self.code].players), 1)

        reply = new.packet("tournament_resumed")
        self.assertEqual(reply["snapshot"]["state"], "waiting")
        self.assertEqual(reply["snapshot"]["time_minutes"], 5)

        await self.svc.disconnect(old)

        self.assertIn(self.code, self.svc.rooms)
        self.assertIs(self.svc.members[self.code][pid], new)

    async def test_replace_active_guest(self):
        await self.register("Host", host=True)
        old, pid, token = await self.register("Guest")

        new = await self.replace_connection(pid, token)

        self.assertTrue(old.closed)
        self.assertIs(self.svc.members[self.code][pid], new)
        self.assertEqual(len(self.svc.rooms[self.code].players), 2)

        await self.svc.disconnect(old)
        self.assertIs(self.svc.members[self.code][pid], new)

    async def test_replace_wrong_token_rejected(self):
        old, pid, token = await self.register("Host", host=True)

        wrong = ("B" if token[0] == "A" else "A") + token[1:]

        with self.assertRaises(PermissionError):
            await self.replace_connection(pid, wrong)

        self.assertFalse(old.closed)
        self.assertIs(self.svc.members[self.code][pid], old)

    async def test_replace_legacy_windows_rejected(self):
        old, pid, token = await self.register(
            "WindowsHost", host=True, mobile=False
        )

        with self.assertRaises(PermissionError):
            await self.replace_connection(pid, token)

        self.assertFalse(old.closed)
        self.assertIs(self.svc.members[self.code][pid], old)

    async def test_replace_without_request_rejected(self):
        old, pid, token = await self.register("Host", host=True)

        with self.assertRaises(PermissionError):
            await self.resume(pid, token)

        self.assertFalse(old.closed)
        self.assertIs(self.svc.members[self.code][pid], old)

    async def test_replace_running_tournament_rejected(self):
        host, pid, token = await self.register("Host", host=True)

        for name in ("Guest1", "Guest2", "Guest3"):
            await self.register(name)

        await self.svc.process(host, {
            "action": "tournament_start",
            "code": self.code
        })

        self.assertTrue(self.svc.rooms[self.code].started)

        with self.assertRaises(PermissionError):
            await self.replace_connection(pid, token)

        self.assertFalse(host.closed)
        self.assertIs(self.svc.members[self.code][pid], host)


if __name__ == "__main__":
    unittest.main(verbosity=2)
