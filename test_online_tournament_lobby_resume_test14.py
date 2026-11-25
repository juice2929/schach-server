import json
import unittest

from online_tournament_ws_clock_dev import TimedTournamentWebSocketService


class MockSocket:
    def __init__(self):
        self.messages = []

    async def send(self, raw):
        self.messages.append(json.loads(raw))

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
