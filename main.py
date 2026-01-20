#!/bin/python3

import asyncio
import json
from aiohttp import web
import websockets
import mysql.connector
import aiomysql
from functools import partial
import secrets
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, messaging
from firebase_admin import exceptions  

# config file local
#import config
from config import SERVER_IP, DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME, SERVER_PORT

connected_clients = {}
pool = None

async def init_db():
    # Connect without specifying DB (to check/create DB)
    conn = await aiomysql.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS
    )
    async with conn.cursor() as cursor:
        # Check if DB exists
        await cursor.execute(f"SHOW DATABASES LIKE '{DB_NAME}'")
        result = await cursor.fetchone()
        if not result:
            print(f"Database {DB_NAME} not found. Creating...")
            await cursor.execute(f"CREATE DATABASE {DB_NAME}")
        else:
            print(f"Database {DB_NAME} already exists.")
    await conn.ensure_closed()

    # Reconnect to the new DB
    conn = await aiomysql.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS,
        db=DB_NAME
    )
    async with conn.cursor() as cursor:
        # Create tables if not exist
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS client_notifications (
                id INT AUTO_INCREMENT PRIMARY KEY,
                organization_id bigint(20) NOT NULL,
                user_id bigint(20) unsigned NOT NULL,
                msg_type INT,
                message TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_organization_id (organization_id),
                INDEX idx_msg_type (msg_type),
                INDEX idx_user_id (user_id)
            )
        """)

        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS room_messages (
                id INT AUTO_INCREMENT PRIMARY KEY,
                organization_id bigint(20) NOT NULL,
                room_id INT NOT NULL,
                user_id bigint(20) unsigned NOT NULL,
                message TEXT NOT NULL,
                is_deleted tinyint(1) DEFAULT 0,
                message_information LONGTEXT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_organization_id (organization_id),
                INDEX idx_room_id (room_id),
                INDEX idx_user_id (user_id)
            )
        """)

        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS rooms (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) DEFAULT NULL,
                status INT DEFAULT 0,
                image VARCHAR(255) DEFAULT NULL,
                description TEXT DEFAULT NULL,
                organization_id bigint(20) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_organization_id (organization_id),
                INDEX idx_status (status)
            )
        """)

        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS room_participants (
                id INT AUTO_INCREMENT PRIMARY KEY,
                room_id INT NOT NULL,
                user_id bigint(20) unsigned NOT NULL,
                last_message_seen INT, 
                organization_id INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_user_id (user_id),
                INDEX idx_organization_id (organization_id),
                INDEX idx_room_id (room_id)
            )
        """)

        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(512) DEFAULT "",
                token VARCHAR(512) NOT NULL,
                organization_id INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_token (token),
                INDEX idx_organization_id (organization_id)
            )
        """)

        await cursor.execute("""
            SHOW COLUMNS FROM room_participants LIKE 'deleted_at'
            """)
        result = await cursor.fetchone()
        if result:
            print(f"✅ Column deleted_at already exists in room_participants.")
        else:
            print(f"⚙️ Adding column deleted_at to room_participants...")
            await cursor.execute(f"""
                ALTER TABLE room_participants
                ADD COLUMN deleted_at TIMESTAMP NULL DEFAULT NULL
                """)
            
        await cursor.execute("""
            SHOW COLUMNS FROM room_participants LIKE 'silent_notifications'
            """)
        result = await cursor.fetchone()
        if result:
            print(f"✅ Column silent_notifications already exists in room_participants.")
        else:
            print(f"⚙️ Adding column silent_notifications to room_participants...")
            await cursor.execute(f"""
                ALTER TABLE room_participants
                ADD COLUMN silent_notifications INT NOT NULL DEFAULT 0
                """)

        await cursor.execute("""
            SHOW COLUMNS FROM clients LIKE 'device_token'
            """)
        result = await cursor.fetchone()
        if result:
            print(f"✅ Column device_token already exists in clients.")
        else:
            print(f"⚙️ Adding column device_token to clients...")
            await cursor.execute(f"""
                ALTER TABLE clients
                ADD COLUMN device_token TEXT DEFAULT NULL
                """)
            
        await cursor.execute("""
            SHOW COLUMNS FROM rooms LIKE 'owner_id'
            """)
        result = await cursor.fetchone()
        if result:
            print(f"✅ Column owner_id already exists in rooms.")
        else:
            print(f"⚙️ Adding column owner_id to rooms...")
            await cursor.execute(f"""
                ALTER TABLE rooms
                ADD COLUMN owner_id bigint(20) DEFAULT 0
                """)

        await cursor.execute("""
            SHOW COLUMNS FROM clients LIKE 'active'
            """)
        result = await cursor.fetchone()
        if result:
            print(f"✅ Column active already exists in clients.")
        else:
            print(f"⚙️ Adding column active to clients...")
            await cursor.execute(f"""
                ALTER TABLE clients
                ADD COLUMN active INT DEFAULT 30
                """)

        print("✅ Tables ensured.")
    await conn.commit()
    await conn.ensure_closed()

async def create_pool():
    pool = await aiomysql.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
        autocommit=True,     # Optional: automatically commit INSERT/UPDATE
        minsize=1,           # Minimum number of connections in the pool
        maxsize=10           # Maximum number of connections
    )
    return pool

def _can_send_message(last_sent_time , cooldown_minutes ) :
    if last_sent_time is None:
        return True  # no previous message
    now = datetime.utcnow()
    elapsed = now - last_sent_time
    return elapsed > timedelta(minutes=cooldown_minutes)

async def get_user_id_using_username( pool, username, organization_id ) :
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SELECT id FROM clients WHERE username = %s AND organization_id = %s LIMIT 1", (username, int(organization_id)))
            result = await cursor.fetchone()
            user_id = result['id'] if result else None
            return user_id

async def can_send_message( pool, user_id, organization_id, room_id ) :
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SELECT silent_notifications FROM room_participants WHERE user_id = %s AND organization_id = %s AND room_id = %s AND deleted_at IS NULL ORDER BY id DESC LIMIT 1", (user_id, int(organization_id), room_id))
            result = await cursor.fetchone()
            silent = result['silent_notifications'] if result else None
            if silent == 1 : 
                return False
            await cursor.execute("SELECT created_at FROM client_notifications WHERE user_id = %s AND organization_id = %s ORDER BY id DESC LIMIT 1", (user_id, int(organization_id)))
            result = await cursor.fetchone()
            last_sent = result['created_at'] if result else None
            return _can_send_message(last_sent, 5)

async def store_send_notification_message (pool, user_id, message, msg_type, organization_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("INSERT INTO client_notifications (user_id, organization_id, message, msg_type) VALUES( %s, %s, %s, %s)", (user_id, int( organization_id), message, msg_type))
            msg_id = cursor.lastrowid
            return msg_id

async def send_general_notifcation_message( pool, user_id, organization_id, msg_title, msg_body, message_data ) :
    data = {
        "type": "notification",
        "data": f"{message_data}"
    }
    print(f"    -->>   In function send_general_notifcation_message {user_id}")
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SELECT device_token FROM clients WHERE id = %s AND organization_id = %s", 
                        (user_id, int(organization_id)))
            result = await cursor.fetchone()
            json_device_tokens = result['device_token'] if result else None
            if( json_device_tokens == None ) :
                return
            try:
                device_tokens = json.loads(json_device_tokens)
            except json.JSONDecodeError:
                print(f"    -->>   send_notifcation_message: Invalid device_token JSON for user {user_id}")
                return
            for tok in device_tokens:
                print(f"    -->>   send_notifcation_message: Sending notification message to {user_id}")
                send_push_notification( tok['token'], msg_title, msg_body, data )
            await store_send_notification_message( pool, user_id, msg_title, 2, organization_id)    

async def send_notifcation_message( pool, user_id, organization_id, msg_title, msg_body, room_id ) :
    data = {
        "type": "chat_msg",
        "data": f"{room_id}"
    }
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SELECT device_token FROM clients WHERE id = %s AND organization_id = %s", (user_id, int(organization_id)))
            result = await cursor.fetchone()
            json_device_tokens = result['device_token'] if result else None
            if( json_device_tokens == None ) :
                return
    
            try:
                device_tokens = json.loads(json_device_tokens)
            except json.JSONDecodeError:
                print(f"send_notifcation_message: Invalid device_token JSON for user {user_id}")
                return
            for tok in device_tokens:
                print(f"send_notifcation_message: Sending notification message to {user_id} ")
                send_push_notification( tok['token'], msg_title, msg_body, data )
    
            await store_send_notification_message( pool, user_id, msg_title, 1, organization_id)    
                
async def get_user_id( username, organization_id ) :
    global pool
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SELECT id FROM clients WHERE username = %s AND organization_id = %s", (username, int(organization_id)))
            user = await cursor.fetchone()
            return user['id'] if user else None  # None if not found, dict if found

async def check_user(username, token):
    global pool
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SELECT * FROM clients WHERE username = %s AND token = %s", (username, token,))
            user = await cursor.fetchone()
            return user  # None if not found, dict if found

async def is_user_room_owner(pool, user_id, room_id, organization_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                SELECT id
                FROM rooms
                WHERE id = %s 
                AND owner_id = %s 
                AND organization_id = %s
            """, (room_id, user_id, organization_id))
            if await cursor.fetchone():
                return True
            else :
                return False

async def get_user_rooms(pool, user_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                SELECT r.id, r.name, r.description, ru.last_message_seen, r.owner_id, ru.silent_notifications
                FROM rooms r 
                JOIN room_participants ru ON ru.room_id = r.id
                WHERE ru.user_id = %s
                AND ru.deleted_at IS NULL
            """, (user_id,))
            rooms = await cursor.fetchall()
            return rooms

async def store_new_message (pool, user_id, message, msginfo, room_id, organization_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("INSERT INTO room_messages (room_id, user_id, organization_id, message, message_information) VALUES( %s, %s, %s, %s, %s)", (room_id, user_id, int( organization_id), message, msginfo))       
            msg_id = cursor.lastrowid
            return msg_id

async def edit_message_in_room (pool, user_id, msg_id, message, msginfo, room_id, organization_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("UPDATE room_messages SET message = %s, message_information = %s WHERE id=%s AND user_id=%s AND room_id=%s AND organization_id=%s", ( message, msginfo, msg_id, user_id, room_id, organization_id))
            return cursor.rowcount

async def update_last_seen_msg_in_room(pool, user_id, room_id, msg_id, organization_id) :
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                UPDATE room_participants 
                SET last_message_seen = %s
                WHERE room_id = %s 
                AND user_id = %s
            """, (msg_id, room_id, user_id))
            await conn.commit()
            if cursor.rowcount > 0:
                return True
            else:
                return False

async def get_last_messages_in_room(pool, user_id, room_id, organization_id) :
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                SELECT m.id, m.user_id, u.username, m.room_id, m.message, m.message_information, m.created_at, m.updated_at
                FROM room_messages m
                JOIN clients u ON m.user_id = u.id
                WHERE m.room_id = %s
                  AND m.organization_id = %s
                  AND m.is_deleted = 0
                ORDER BY m.id DESC
                LIMIT 20
            """, (room_id, organization_id))
            msgs = await cursor.fetchall()

            for msg in msgs:
                for field in ("created_at", "updated_at"):
                    if isinstance(msg.get(field), datetime):
                        msg[field] = msg[field].isoformat()

            return msgs

async def delete_message_in_room(pool, user_id, room_id, msg_id, organization_id) :
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                UPDATE room_messages m
                  SET is_deleted = 1
                WHERE room_id = %s
                  AND organization_id = %s
                  AND user_id = %s
                  AND id = %s
            """, (room_id, organization_id, user_id, msg_id))
            await conn.commit()
            if cursor.rowcount > 0:
                return True
            else:
                return False

        
async def get_prev_messages_in_room(pool, user_id, room_id, organization_id, last_id) :
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                SELECT m.id, m.user_id, u.username, m.room_id, m.message, m.message_information, m.created_at, m.updated_at
                FROM room_messages m
                JOIN clients u ON m.user_id = u.id
                WHERE m.room_id = %s
                  AND m.organization_id = %s
                  AND m.is_deleted = 0
                  AND m.id < %s
                ORDER BY m.id DESC
                LIMIT 20
            """, (room_id, organization_id, last_id))
            msgs = await cursor.fetchall()
            for msg in msgs:
                for field in ("created_at", "updated_at"):
                    if isinstance(msg.get(field), datetime):
                        msg[field] = msg[field].isoformat()
            return msgs
        
async def get_messages_in_room(pool, user_id, room_id, organization_id, last_id) :
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                SELECT m.id, m.user_id, u.username, m.room_id, m.message, m.message_information, m.created_at, m.updated_at
                FROM room_messages m
                JOIN clients u ON m.user_id = u.id
                WHERE m.room_id = %s
                  AND m.organization_id = %s
                  AND m.is_deleted = 0
                  AND m.id > %s
                ORDER BY m.id ASC
                LIMIT 20
            """, (room_id, organization_id, last_id))
            msgs = await cursor.fetchall()
            for msg in msgs:
                for field in ("created_at", "updated_at"):
                    if isinstance(msg.get(field), datetime):
                        msg[field] = msg[field].isoformat()
            return msgs
        
async def get_users_in_room(pool, room_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                SELECT user_id
                FROM room_participants 
                WHERE room_id = %s
                AND deleted_at IS NULL
            """, (room_id,))        
            rows = await cursor.fetchall()
            return [row['user_id'] for row in rows]  # return only the IDs

async def leave_room ( pool, room_id, user_id) :
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                UPDATE room_participants 
                SET deleted_at = NOW() 
                WHERE room_id = %s
                AND user_id = %s
            """, (room_id, user_id,))
            if cursor.rowcount > 0:
                return True
            else :
                return False

async def silent_room ( pool, room_id, user_id) :
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                UPDATE room_participants
                SET silent_notifications = 1 
                WHERE room_id = %s
                AND user_id = %s
            """, (room_id, user_id,))
            if cursor.rowcount > 0:
                return True
            else :
                return False
            
async def unsilent_room ( pool, room_id, user_id) :
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                UPDATE room_participants
                SET silent_notifications = 0
                WHERE room_id = %s
                AND user_id = %s
            """, (room_id, user_id,))
            if cursor.rowcount > 0:
                return True
            else :
                return False

async def get_room_owner(pool, room_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                SELECT u.id, u.username
                FROM rooms r
                JOIN clients u ON r.owner_id = u.id
                WHERE r.id = %s
            """, (room_id,))
            users = await cursor.fetchall()
            for user in users:
                uid = user['id']
                user["online"] = isUserOnline( uid )
            return users

async def get_user_names_in_room(pool, room_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                SELECT u.id, u.username
                FROM room_participants rp
                JOIN clients u ON rp.user_id = u.id
                WHERE rp.room_id = %s
                AND rp.deleted_at IS NULL
            """, (room_id,))
            users = await cursor.fetchall()
            for user in users:
                uid = user['id']
                user["online"] = isUserOnline( uid )
            return users

async def mark_msg_not_read(pool, user_ids, room_id, msg_id):
    if not user_ids:
        return
    placeholders = ','.join(['%s'] * len(user_ids))
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            sql = f"""
                UPDATE room_participants 
                SET last_message_seen = {msg_id} - 1
                WHERE room_id = %s 
                AND user_id IN ({placeholders})
            """
            await cursor.execute(sql, (room_id, *user_ids)) # TODO might be hard on the database if there are many people in room
            await conn.commit()

async def clear_user_last_seen_msg(pool, user_id, room_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            sql = f"""
                UPDATE room_participants 
                SET last_message_seen = 0
                WHERE room_id = %s AND user_id = %s
                AND deleted_at IS NULL
            """
            await cursor.execute(sql, (room_id, user_id))
            await conn.commit()

async def create_or_update_room(pool, user_id, room_name, user_ids, description, organization_id):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT id FROM rooms WHERE name = %s AND organization_id = %s", (room_name,organization_id))
            existing_room = await cursor.fetchone()
            if existing_room:
                room_id = existing_room[0]
                if await is_user_room_owner(pool, user_id, room_id, organization_id) == False :
                    return None
                # Update room info
                if organization_id:
                    await cursor.execute( "UPDATE rooms SET description = %s, name = %s WHERE id = %s", (description, room_name, room_id))
                # Remove old users
                await cursor.execute("DELETE FROM room_participants WHERE room_id = %s", (room_id,))
            else:
                # Insert new room
                if organization_id:
                    await cursor.execute( "INSERT INTO rooms (name, organization_id, description, owner_id) VALUES (%s, %s, %s, %s)", (room_name, int(organization_id), description, user_id))
                else:
                    await cursor.execute( "INSERT INTO rooms (name, description, owner_id) VALUES (%s,%s)", (room_name,description, user_id))
                room_id = cursor.lastrowid

            # Add users to the room
            found = False
            for uid in user_ids:
                if not isinstance(uid, str) or uid.isdigit() == False :
                    uid = await get_user_id(uid, organization_id)
                if uid is None:
                    continue
                if uid == user_id :
                    found = True
                await cursor.execute( "INSERT INTO room_participants (room_id, user_id, last_message_seen, organization_id) VALUES (%s, %s, %s, %s)", (room_id, uid, 0, int( organization_id )))

            if( found == False ):
                await cursor.execute( "INSERT INTO room_participants (room_id, user_id, last_message_seen, organization_id) VALUES (%s, %s, %s, %s)", (room_id, user_id, 0, int(organization_id)))

            await conn.commit()
            return room_id

def isUserOnline( user_id ):
    for ws, info in connected_clients.items():
        if info.get("user_id") == user_id :
            return True
    return False

async def send_general_notification_msg_to_users( pool, message, user_id, organization_id, msg_title, msg_body ):
    tasks = []
    found = False
    print(f"   -->>    In function send_general_notification_msg_to_users {user_id} ")
    for ws, info in connected_clients.items():
        ws_user_id = info.get("user_id")
        if(user_id == ws_user_id) :
            found = True
            tasks.append(ws.send(message))
            break

    if found == False:
        print(f"    -->>   sending to {user_id} is offline using firebase")
        await send_general_notifcation_message( pool, user_id, organization_id, msg_title, msg_body, message ) 
    if tasks:
        print(f"   -->>    sending to {user_id} is online - using websockets")
        await asyncio.gather(*tasks, return_exceptions=True)

async def send_msg_to_users( pool, message, user_ids, organization_id, room_id ):
    tasks = []
    for user_id in user_ids:
        found = False
        for ws, info in connected_clients.items():
            ws_user_id = info.get("user_id")
            if(user_id == ws_user_id) :
                found = True
                tasks.append(ws.send(message))
                break
        if found == False:
            if await can_send_message(pool, user_id, organization_id, room_id ):
                await send_notifcation_message( pool, user_id, organization_id, "New Message", "A new chat message is sent to you", room_id )
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# WebSocket server
async def ws_handler( websocket ):
    global pool

    connected_clients[websocket] = {"registered": False, "user": None}
    registered = False
    
    # Prefer proxy-provided client IPs when behind nginx.
    headers = None
    if hasattr(websocket, "request_headers"):
        headers = websocket.request_headers
    elif hasattr(websocket, "request") and websocket.request is not None:
        headers = websocket.request.headers
    forwarded_for = headers.get("X-Forwarded-For") if headers else None
    if forwarded_for:
        client_ip = forwarded_for.split(",")[0].strip()
    else:
        client_ip = headers.get("X-Real-IP") if headers else None
    client_ip = client_ip or websocket.remote_address[0]
    client_port = websocket.remote_address[1]
    print(f"{client_ip}:{client_port}: New socket connection")

    try:
        async for message in websocket:
            try:
                print(f"Got message {message}")
                theMessageContent = json.loads(message)
            except json.JSONDecodeError:
                await websocket.send(json.dumps({"error": "Invalid JSON"}))
                continue

            try:
                client_info =connected_clients[websocket]
                event = theMessageContent.get("event")
                print(f"Request Event from {client_ip} '{event}'")

                #resgiter client # Param are: user_id
                if not client_info["registered"]:
                    event = theMessageContent.get("event")
                    if event=="Register" :
                        print(f"{client_ip}: Event '{event}'")
                        username = theMessageContent.get("username")
                        token = theMessageContent.get("token")
                        print(f"{client_ip}: user_id '{username}'")
                        user = await check_user(username, token)
                        if user == None :
                            await websocket.send(json.dumps({
                                "event":"register_error",
                                "data":"invalid user"}))
                            continue

                        client_info["session_token"] = secrets.token_urlsafe(32)
                        client_info["organization_id"] = user["organization_id"]
                        client_info["registered"] = True
                        client_info["user_id"] = user['id']
                        client_info["username"] = user['username']

                        await websocket.send( json.dumps({
                            "event":"register_success",
                            "data":client_info['session_token']}))
                    else:
                        print(f"{client_ip}: Event '{event}'")
                        print(f"{client_ip}: Client not registered yet")
                        await websocket.send(json.dumps({
                            "event":"register_error",
                            "data":"You must send a register event first"}))                
                    continue

                ## send notifications to clients
                if event == "notification":
                    data = theMessageContent.get("data") or {}
                    session_token = data.get('session_token')
                    if client_info['session_token'] != session_token :
                        print(f"{client_ip}: invalid token Session token is invalid")
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue

                    organization_id = theMessageContent.get("organization_id")
                    if organization_id is None:
                        print(f"{client_ip}: invalid organization id")
                        await websocket.send(json.dumps({
                            "error":"invalid organization id",
                            "data":"organization id is missing"
                        }))
                        continue

                    org_id = client_info['organization_id']
                    if int(org_id) > 0 and int(org_id) != int(organization_id) :
                        print(f"{client_ip}: invalid organization id does not match client organization id")
                        await websocket.send(json.dumps({
                            "error":"invalid organization id",
                            "data":"invalid organization id"
                        }))
                        continue

                    username = theMessageContent.get("username")
                    user_id = await get_user_id_using_username(pool, username, organization_id)
                    if user_id != None:
                        msg_title = theMessageContent.get("title")
                        msg_body = theMessageContent.get("body")
                        if isinstance(data, dict) and "notification" in data:
                            payload = data["notification"]
                        else:
                            payload = data if isinstance(data, str) else json.dumps(data)
                        
                        notification_message = json.dumps({
                            "event": "notification",
                            "data": {
                                "title": msg_title,
                                "body": msg_body,
                                "message": data['notification'],
                            }
                        })
                        await send_general_notification_msg_to_users(pool, notification_message, user_id, organization_id, msg_title, msg_body)
                        await websocket.send( json.dumps({
                            "event":"notification_success",
                            }))
                    else :
                        print(f"{client_ip}: username is not found")
                        await websocket.send( json.dumps({
                            "event":"notification_failed",
                            "data":"username is not found"}))

                ## get list of rooms  param: session_token
                if event == "GetRooms":
                    data = theMessageContent.get("data")
                    session_token = data['session_token']
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue

                    rooms = await get_user_rooms(pool, client_info['user_id'])
                    if rooms == None :
                        await websocket.send(json.dumps({
                            "event":"get_rooms_failed",
                            "data":"User not registered in any rooms"}))
                    else : 
                        await websocket.send(json.dumps({
                            "event": "get_rooms",
                            "data": rooms 
                            }))
                    continue

                ## create a rooms param: session_token, name, users, description
                if event == "UpdateOrMakeRoom":
                    data = theMessageContent.get("data")
                    session_token = data['session_token']
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue

                    user_id = client_info['user_id']
                    room_name = data["name"]
                    user_names = data["users"]
                    description = data["description"]
                    org_id = client_info['organization_id']
                    room_id = await create_or_update_room( pool, user_id, room_name, user_names, description, org_id )
                    if room_id == None: 
                        await websocket.send(json.dumps({
                            "event":"update_or_make_room",
                            "data":{ 
                                "room": room_id,
                                "status": "failed",
                                "msg":"Failed to create a room"
                                }
                            }))
                    else:
                        await websocket.send(json.dumps({
                            "event":"update_or_make_room",
                            "data":{
                                "room": room_id,
                                "name": room_name,
                                "status": "success"
                                }
                            }))

                ## get the users in a room -- param: session_token, room id
                if event == "GetUsersInRoom" :
                    data = theMessageContent.get("data")
                    session_token = data['session_token']
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue

                    room_id = data['room']
                    owners = await get_room_owner(pool, room_id)
                    users = await get_user_names_in_room( pool, room_id )
                    await websocket.send(json.dumps({
                        "event":"room_users",
                        "room":room_id,
                        "users":users or [],
                        "owners":owners or []
                    }))

                ## leave room -- param: session_token, room id
                if event == "LeaveRoom" :
                    data = theMessageContent.get("data")
                    session_token = data['session_token']
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue
                    
                    user_id = client_info['user_id']
                    res = await leave_room( pool, data['room'], user_id )
                    if( res == True ) :
                        await websocket.send(json.dumps({
                            "event":"leave_room_success",
                        }))
                    else : 
                        await websocket.send(json.dumps({
                            "event":"leave_room_failed",
                        }))

                ###
                if event == "SilentRoom" :
                    data = theMessageContent.get("data")
                    session_token = data['session_token']
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue
                    
                    user_id = client_info['user_id']
                    res = await silent_room( pool, data['room'], user_id )
                    if( res == True ) :
                        await websocket.send(json.dumps({
                            "event":"silent_room_success",
                        }))
                    else : 
                        await websocket.send(json.dumps({
                            "event":"silent_room_failed",
                        }))

                ###
                if event == "UnSilentRoom" :
                    data = theMessageContent.get("data")
                    session_token = data['session_token']
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue
                    
                    user_id = client_info['user_id']
                    res = await unsilent_room( pool, data['room'], user_id )
                    if( res == True ) :
                        await websocket.send(json.dumps({
                            "event":"unsilent_room_success",
                        }))
                    else : 
                        await websocket.send(json.dumps({
                            "event":"unsilent_room_failed",
                        }))
                    
                ## Clear the last seen -- param: session_token, room id
                if event == "ClearLastMessageSeen":
                    data = theMessageContent.get("data")
                    session_token = data['session_token']
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue

                    room_id = data['room']
                    user_id = client_info['user_id']
                    await clear_user_last_seen_msg( pool, user_id, room_id )
                    await websocket.send(json.dumps({
                            "event":"cleared_last_seen_msgs",
                            "data":""
                        }))

                ## Get all msgs in room after specific msg --- param: session_token, room id, last msg seen
                if event == "GetMessagesInRoom":
                    data = theMessageContent.get("data")
                    session_token = data['session_token']
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue
                    room_id = data['room']
                    last_id = data['last_id']
                    user_id = client_info['user_id']
                    organization_id = client_info['organization_id']

                    msgs = await get_messages_in_room( pool, user_id, room_id, organization_id, last_id )
                    await websocket.send(json.dumps({
                            "event":"messages_in_room",
                            "data": msgs
                        }))
                    
                ## Get all msgs in room before a specific msg --- param: session_token, room id, last msg seen
                if event == "GetPrevMessagesInRoom":
                    data = theMessageContent.get("data")
                    session_token = data['session_token']
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue
                    room_id = data['room']
                    last_id = data['last_id']
                    user_id = client_info['user_id']
                    organization_id = client_info['organization_id']
                    msgs = await get_prev_messages_in_room( pool, user_id, room_id, organization_id, last_id )
                    await websocket.send(json.dumps({
                            "event":"prev_messages_in_room",
                            "data": msgs
                        }))
                    
                ## Get the last messages in a room --- param: session_token, room id, last msg seen
                if event == "GetLastMessagesInRoom":
                    data = theMessageContent.get("data")
                    session_token = data['session_token']
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue
                    room_id = data['room']
                    user_id = client_info['user_id']
                    organization_id = client_info['organization_id']

                    msgs = await get_last_messages_in_room( pool, user_id, room_id, organization_id )
                    await websocket.send(json.dumps({
                            "event":"last_messages_in_room",
                            "data": msgs
                        }))
                    
                ## delete msg in room --- param: session_token, room id, msg_id
                if event == "DeleteMessageInRoom":
                    data = theMessageContent.get("data")
                    session_token = data['session_token']
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue
                    room_id = data['room']
                    user_id = client_info['user_id']
                    msg_id = data['msg_id']
                    organization_id = client_info['organization_id']

                    res = await delete_message_in_room( pool, user_id, room_id, msg_id, organization_id )
                    await websocket.send(json.dumps({
                            "event":"delete_messages_in_room",
                            "success": res
                        }))
                    
                ## edit msg in room --- param: session_token, room id, msg_id
                if event == "EditMessageInRoom":
                    data = theMessageContent.get("data")
                    session_token = data['session_token']
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue

                    room_id = data['room']
                    user_id = client_info['user_id']
                    msg_id = data['msg_id']
                    organization_id = client_info['organization_id']
                    msg = data['message']
                    info = data['msginfo']

                    result = await edit_message_in_room( pool, user_id, msg_id, msg, info, room_id, organization_id )
                    if(result > 0 ) :
                        await websocket.send(json.dumps({
                                "event":"edit_message_in_room",
                                "data": result
                            }))
                        
                        user_ids = await get_users_in_room( pool, room_id )

                        #broadcast it to online users
                        broadcast_data = json.dumps({
                            "event": "chat_message_updated",
                            "data": {
                                "username": client_info['username'],
                                'msgid': msg_id,
                                "room":room_id,
                                "message": data['message'],
                                "msginfo": data['msginfo'],
                            }
                        })
                        await send_msg_to_users( pool, broadcast_data, user_ids, organization_id, room_id )

                    else :
                        await websocket.send(json.dumps({
                                "event":"edit_message_in_room",
                                "data": "failed"
                            }))
                    
                if event == "Ping":
                    data = theMessageContent.get("data") or {}
                    session_token = data.get('session_token')
                    if not session_token:
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is missing"
                        }))
                        continue
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue
                    await websocket.send(json.dumps({
                        "event":"ping_response",
                        "status": True,
                        "user_id": client_info['user_id']
                        }))

                if event == "GetUserStatus":
                    data = theMessageContent.get("data") or {}
                    session_token = data.get('session_token')
                    if not session_token:
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is missing"
                        }))
                        continue
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue
                    user_id = client_info['user_id']
                    await websocket.send(json.dumps({
                        "event":"user_status_response",
                        "user_id": user_id,
                        "status": isUserOnline(user_id),
                        }))
                    
                ## got the event and payload
                if event == "LastSeenMsg":
                    data = theMessageContent.get("data")
                    session_token = data['session_token']
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue

                    room_id = data['room']
                    user_id = client_info['user_id']
                    msg_id = data['msg_id']
                    organization_id = client_info['organization_id']
                    result = await update_last_seen_msg_in_room( pool, user_id, room_id, msg_id, organization_id )
                    await websocket.send(json.dumps({
                        "event":"update_last_seen_msg_in_room",
                        "status": result
                    }))
                    
                ## got the event and payload
                if event == "BroadcastMessage":
                    data = theMessageContent.get("data")
                    session_token = data['session_token']
                    if client_info['session_token'] != session_token :
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue

                    user_id = client_info['user_id']
                    organization_id = client_info['organization_id']
                    room_id = data['room']
                    user_ids = await get_users_in_room( pool, room_id )
                    print(f"{user_id}: Got BroadcastMessage ")

                    if user_id in user_ids:
                        user_ids.remove(user_id)
                    else:
                        await websocket.send(json.dumps({
                            "event":"broadcast_message_response",
                            "status": False
                        }))
                        continue

                    #store the msg for offline users
                    id = await store_new_message(pool, user_id, data['message'], data['msginfo'], room_id, client_info['organization_id'])

                    #broadcast it to online users
                    broadcast_data = json.dumps({
                        "event": "chat_message",
                        "data": {
                            "username": client_info['username'],
                            'msgid': id,
                            "room":room_id,
                            "message": data['message'],
                            "msginfo": data['msginfo'],
                        }
                    })
                    await send_msg_to_users( pool, broadcast_data, user_ids, organization_id, room_id )

                    await websocket.send(json.dumps({
                            "event":"broadcast_message_response",
                            "status": True,
                            "msgid": id
                        }))

            except Exception as outer_err:
                # Catch websocket errors (disconnects, etc.)
                print(f"WebSocket error: {outer_err}")
    except websockets.ConnectionClosed:
        pass
    finally:
        connected_clients.pop(websocket, None)

def send_push_notification(token, title, body, data=None):
    """
    Sends a push notification to a specific device via FCM.
    :param token: The FCM device registration token.
    :param title: Notification title.
    :param body: Notification body.
    :param data: Optional custom key/value payload (dict).
    """
    # Create the message
    message = messaging.Message(
        notification=messaging.Notification(
            title=title,
            body=body,
        ),
        token=token,
        data=data or {}
    )

    # Send message
    response = messaging.send(message)
    print(f"✅ Successfully sent message with token: {token} response:  {response}")


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
    global pool

    try:
        cred = credentials.Certificate("firebase_credentials.json")
        firebase_admin.initialize_app(cred)
        print("Firebase initialized successfully")

    except ValueError as e:
        # Happens if Firebase app is already initialized
        print("Firebase already initialized:", e)

    except FileNotFoundError as e:
        # Happens if the service account file path is wrong
        print("Credential file not found:", e)

    except exceptions.FirebaseError as e:
        # Catches Firebase-specific errors (e.g., invalid credentials)
        print("Firebase initialization error:", e)

    except Exception as e:
        # Generic catch-all for anything unexpected
        print("Unexpected error:", e)

    await init_db()
    pool = await create_pool()

    # Start WebSocket server on port 8080
    #ws_server = await websockets.serve(lambda ws, path:ws_handler(ws, path, pool), SERVER_IP, 8080)
    ws_server = await websockets.serve(ws_handler, SERVER_IP, SERVER_PORT)

    # Start HTTP server on port 8081
    #app = web.Application()
    #app.add_routes([web.post('/sendmessage', http_sendmessage)])
    #runner = web.AppRunner(app)
    #await runner.setup()
    #site = web.TCPSite(runner, SERVER_IP, 8081)
    #await site.start()

    print(f"WebSocket: ws://{SERVER_IP}:{SERVER_PORT}")
    #print("HTTP POST: http://{SERVER_IP}:8081/sendmessage")
    await asyncio.Future()  # run forever


asyncio.run(main())
