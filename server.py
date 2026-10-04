import asyncio
import json
import websockets

# Speichert aktive Räume: { "RAUMCODE": { "p1": websocket, "p2": websocket } }
rooms = {}

async def handler(websocket):
    room_id = None
    try:
        async for message in websocket:
            data = json.loads(message)
            action = data.get("action")

            # Spieler 1 erstellt einen Raum
            if action == "create":
                room_id = data.get("room")
                rooms[room_id] = {"p1": websocket, "p2": None}
                await websocket.send(json.dumps({"status": "waiting"}))

            # Spieler 2 tritt dem Raum bei
            elif action == "join":
                room_id = data.get("room")
                if room_id in rooms and rooms[room_id]["p2"] is None:
                    rooms[room_id]["p2"] = websocket
                    await websocket.send(json.dumps({"status": "paired"}))
                    # Benachrichtige Spieler 1, dass das Spiel startet
                    await rooms[room_id]["p1"].send(json.dumps({"status": "paired"}))
                else:
                    await websocket.send(json.dumps({"status": "error", "message": "Raum nicht gefunden"}))

            # Schachzug weiterleiten
            elif action == "move":
                room_id = data.get("room")
                if room_id in rooms:
                    p1 = rooms[room_id]["p1"]
                    p2 = rooms[room_id]["p2"]
                    target = p2 if websocket == p1 else p1
                    if target:
                        await target.send(json.dumps({"action": "move", "move": data.get("move")}))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if room_id and room_id in rooms:
            del rooms[room_id]

async def main():
    # Lauscht auf Port 8765
    async with websockets.serve(handler, "0.0.0.0", 8765):
        print("Relay-Server läuft...")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())