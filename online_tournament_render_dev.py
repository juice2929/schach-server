import asyncio
import os
import websockets

from online_tournament_ws_clock_dev import TimedTournamentWebSocketService

async def main():
    host = os.environ.get("TOURNAMENT_HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "10003"))

    service = TimedTournamentWebSocketService()

    async with websockets.serve(
        service.handler, host, port, max_size=4096
    ):
        print(f"Tournament server: {host}:{port}", flush=True)
        await service.monitor()

if __name__ == "__main__":
    asyncio.run(main())
