import asyncio
import json
import websockets

ROOMS = {}  # room_code -> Liste von websockets


async def handler(websocket):
    current_room = None
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                if isinstance(data, dict) and "room" in data:
                    current_room = str(data["room"])
                    if current_room not in ROOMS:
                        ROOMS[current_room] = []

                    if websocket not in ROOMS[current_room]:
                        ROOMS[current_room].append(websocket)

                    print(
                        f"Spieler in Raum '{current_room}'. Gesamt: {len(ROOMS[current_room])}"
                    )

                    # WICHTIG: Wenn 2 Spieler im Raum sind, sende 'start' / Raum-Bestätigung an BEIDE!
                    if len(ROOMS[current_room]) >= 2:
                        for client in ROOMS[current_room]:
                            await client.send(
                                json.dumps(
                                    {"status": "start", "room": current_room}
                                )
                            )
                    else:
                        # Bestätigung für den 1. Spieler
                        await websocket.send(
                            json.dumps(
                                {"status": "waiting", "room": current_room}
                            )
                        )
                    continue
            except Exception:
                pass

            # Leite alle anderen Nachrichten (Züge, Chat) an den jeweils anderen Spieler weiter
            if current_room and current_room in ROOMS:
                for client in ROOMS[current_room]:
                    if client != websocket:
                        await client.send(message)

    except Exception as e:
        print(f"Fehler: {e}")
    finally:
        if current_room and current_room in ROOMS:
            if websocket in ROOMS[current_room]:
                ROOMS[current_room].remove(websocket)


async def main():
    async with websockets.serve(handler, "0.0.0.0", 10000):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())