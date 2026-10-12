import asyncio
import json
import websockets

ROOMS = {}

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
                    
                    # Wenn 2 Spieler da sind, benachrichtige sie, aber ohne das Spiel-Format zu stören
                    if len(ROOMS[current_room]) >= 2:
                        for client in ROOMS[current_room]:
                            await client.send(json.dumps({"action": "start", "room": current_room}))
                    continue
            except Exception:
                pass

            # Leite normale Züge/Nachrichten an den Partner weiter
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
