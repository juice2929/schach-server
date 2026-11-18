import asyncio
import logging
import os
import websockets

from online_tournament_reliable_dev import ReliableTournamentService


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(message)s"
    )

    host = os.environ.get("TOURNAMENT_HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "10003"))

    service = ReliableTournamentService()

    async with websockets.serve(
        service.handler,
        host,
        port,
        max_size=4096,
        ping_interval=5,
        ping_timeout=5,
        close_timeout=2
    ):
        print(f"Tournament reconnect server: {host}:{port}", flush=True)
        await service.monitor()


if __name__ == "__main__":
    asyncio.run(main())