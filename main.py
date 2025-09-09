#!/bin/python3

import asyncio
import json
from aiohttp import web
import websockets
import mysql.connector
import aiomysql

connected_clients = {}

async def create_pool():
    pool = await aiomysql.create_pool(
        host='172.105.19.30',
        port=3306,
        user='client1',
        password='pZL8giwz9vgvA3PuU4n',
        db='chat_web',
        autocommit=True,     # Optional: automatically commit INSERT/UPDATE
        minsize=1,           # Minimum number of connections in the pool
        maxsize=10           # Maximum number of connections
    )
    return pool

async def check_user(pool, username):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SELECT * FROM list_users WHERE name = %s", (username,))
            user = await cursor.fetchone()
            return user  # None if not found, dict if found

async def get_user_rooms(pool, user_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                SELECT r.id, r.name, r.description
                FROM rooms r
                JOIN room_users ru ON ru.room_id = r.id
                WHERE ru.user_id = %s
            """, (user_id,))
            rooms = await cursor.fetchall()
            return rooms


async def create_or_update_room(pool, room_name, user_ids, description, organization_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT id FROM rooms WHERE name = %s", (room_name,))
            existing_room = await cursor.fetchone()
            if existing_room:
                room_id = existing_room[0]
                # Update room info
                if organization_id:
                    await cursor.execute( "UPDATE rooms SET description = '%s', organization_id = %s WHERE id = %s", (description, organization_id, room_id))
                # Remove old users
                await cursor.execute("DELETE FROM room_users WHERE room_id = %s", (room_id,))
            else:
                # Insert new room
                if organization_id:
                    await cursor.execute( "INSERT INTO rooms (name, organization_id, description) VALUES (%s, %s, '%s')", (room_name, organization_id, description))
                else:
                    await cursor.execute( "INSERT INTO rooms (name, description) VALUES (%s,%s)", (room_name,description))
                room_id = cursor.lastrowid

            # Add users to the room
            for user_id in user_ids:
                await cursor.execute( "INSERT INTO room_users (room_id, user_id) VALUES (%s, %s)", (room_id, user_id))

            await conn.commit()
            return room_id

# WebSocket server
async def ws_handler(websocket, pool):

    cursor = db_conn.cursor(dictionary=True)
    connected_clients[websocket] = {"registered": False, "user": None}
    registered = False

    try:
        async for message in websocket:
            try: 
                theMessageContent = json.loads(message)
            except json.JSONDecodeError:
                await websocket.send(json.dumps({"error": "Invalid JSON"}))
                continue

            client_info =connected_clients[websocket]
            event = theMessageContent.get("event")

            #resgiter client
            if not client_info["registered"]:
                event = theMessageContent.get("event")
                if event=="register" :
                    username = theMessageContent.get("username")
                    user = check_user(pool, username)
                    if user == None :
                        await websocket.send(json.dumps({
                            "event":"register_error",
                            "data":"invalid user"}))
                        continue;

                    client_info["register"] = True
                    client_info["user_id"] = user["name"] 
                    client_info["organization_id"] = user["organization_id"] 
                    await websocket.send( json.dumps({
                        "event":"register_success",
                        "data":""}))
                else:
                    await websocket.send(json.dumps({
                        "event":"register_error",
                        "data":"You must send a register event first"}))
                continue

            ## get list of rooms 
            if event == "GetRooms":
                rooms = await get_user_rooms(pool, client_info['user_id'])
                if rooms == None :
                    await websocket.send(json.dumps({
                        "event":"GetRooms",
                        "status":"failed",
                        "data":"User not registered in any rooms"}))
                else : 
                    await websocket.send(json.dumps({
                        "event": "GetRooms",
                        "status":"success",
                        "data": rooms }))
                continue

            ## create a rooms 
            if event == "UpdateOrMakeRoom":
                room_name = theMessageContent.get("name")
                user_names = theMessageContent.get("users")
                description = theMessageContent.get("description")
                room_id = await create_or_update_room( pool, room_name, user_names, description, client_info['organization_id'] )
                if room_id == None: 
                    await websocket.send(json.dumps({
                        "event":"UpdateOrMakeRoom",
                        "status":"failed",
                        "data":"Failed to create a room"}))
                else:
                    await websocket.send(json.dumps({
                        "event":"UpdateOrMakeRoom",
                        "status":"success"}))

            ## got the event and payload
            if event == "Message":
                room = theMessageContent.get("room")
                broadcast_data = json.dumps({
                    "event": "ChatMessageSent",
                    "data": payload
                })
                await asyncio.gather(*[
                    client.send(broadcast_data)
                    for client in connected_clients
                    if client != websocket
                ])
    except websockets.ConnectionClosed:
        pass
    finally:
        connected_clients.remove(websocket)

# HTTP POST server
async def http_sendmessage(request):
    data = await request.post()
    user = data.get("user", "Console")
    message = data.get("message", "")
    broadcast_data = json.dumps({
        "event": "ChatMessageSent",
        "data": {"user": user, "message": message}
    })
    await asyncio.gather(*[
        client.send(broadcast_data)
        for client in connected_clients
    ])
    return web.json_response({"status": "ok"})

async def main():
    pool = await create_pool()

    # Start WebSocket server on port 8080
    ws_server = await websockets.serve(lambda ws, path:ws_handler(ws, pool), "0.0.0.0", 8080)

    # Start HTTP server on port 8081
    app = web.Application()
    app.add_routes([web.post('/sendmessage', http_sendmessage)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8081)
    await site.start()

    print("WebSocket: ws://0.0.0.0:8080")
    print("HTTP POST: http://0.0.0.0:8081/sendmessage")
    await asyncio.Future()  # run forever

asyncio.run(main())



