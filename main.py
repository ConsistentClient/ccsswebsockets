#!/bin/python3

import asyncio
import json
import hashlib
import logging
import os
from aiohttp import web
import websockets
import mysql.connector
import aiomysql
from functools import partial
import secrets
from types import SimpleNamespace
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, messaging
from firebase_admin import exceptions  
from pbx_notifications import CallNotificationDispatcher, create_pbx_app
from chat_http import create_chat_app

# config file local
import config
from config import SERVER_IP, DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME, SERVER_PORT, NOTIFY_USER, NOTIFY_USER_PATH

connected_clients = {}
pool = None
call_notification_dispatcher = None
call_notification_tasks = set()

# StreamHandler writes to stderr, including INFO diagnostics in the service err log.
notification_logger = logging.getLogger("firebase_notifications")
notification_logger.setLevel(logging.INFO)
notification_logger.propagate = False
if not notification_logger.handlers:
    notification_handler = logging.StreamHandler()
    notification_handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s [firebase_notifications] %(message)s"
    ))
    notification_logger.addHandler(notification_handler)


def device_token_fingerprint(token):
    if not isinstance(token, str) or not token.strip():
        return "missing_or_invalid"
    return hashlib.sha256(token.encode()).hexdigest()[:12]


def notification_device_tokens(result, user_id, organization_id):
    context = "user_id=%s organization_id=%s"
    if not result:
        notification_logger.warning("Push skipped: client row not found; " + context, user_id, organization_id)
        return None
    raw_tokens = result.get("device_token")
    if raw_tokens is None:
        notification_logger.warning("Push skipped: no device tokens registered; " + context, user_id, organization_id)
        return None
    try:
        tokens = json.loads(raw_tokens)
    except (json.JSONDecodeError, TypeError):
        notification_logger.warning("Push skipped: invalid device_token JSON; " + context, user_id, organization_id)
        return None
    if not isinstance(tokens, list):
        notification_logger.warning("Push skipped: device_token JSON must be a list; " + context, user_id, organization_id)
        return None
    notification_logger.info("Device token lookup: " + context + " token_count=%s", user_id, organization_id, len(tokens))
    if not tokens:
        notification_logger.warning("Push skipped: device token list is empty; " + context, user_id, organization_id)
    return tokens


def send_device_notifications(device_tokens, user_id, organization_id, title, body, data):
    invalid_tokens = []
    counts = {"ok": 0, "unregistered": 0, "error": 0}
    for index, entry in enumerate(device_tokens):
        token = entry.get("token") if isinstance(entry, dict) else None
        if not isinstance(token, str) or not token.strip():
            notification_logger.warning(
                "Device entry skipped: missing/invalid token user_id=%s organization_id=%s device_index=%s",
                user_id, organization_id, index,
            )
            counts["error"] += 1
            continue
        status = send_push_notification(token, title, body, data, user_id, organization_id)
        counts[status] += 1
        if status == "unregistered":
            invalid_tokens.append(token)
    notification_logger.info(
        "Push summary: user_id=%s organization_id=%s type=%s accepted=%s unregistered=%s failed_or_skipped=%s",
        user_id, organization_id, data.get("type"), counts["ok"], counts["unregistered"], counts["error"],
    )
    return invalid_tokens

async def init_db():
    notify_user = NOTIFY_USER
    notify_user_path = NOTIFY_USER_PATH

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
                room_type VARCHAR(16) NOT NULL DEFAULT 'group',
                status INT DEFAULT 0,
                image VARCHAR(255) DEFAULT NULL,
                description TEXT DEFAULT NULL,
                organization_id bigint(20) DEFAULT 0,
                last_message_at TIMESTAMP NULL DEFAULT NULL,
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
            SHOW COLUMNS FROM rooms LIKE 'last_message_at'
            """)
        result = await cursor.fetchone()
        if result:
            print(f"✅ Column last_message_at already exists in rooms.")
        else:
            print(f"⚙️ Adding column last_message_at to rooms...")
            await cursor.execute(f"""
                ALTER TABLE rooms
                ADD COLUMN last_message_at TIMESTAMP NULL DEFAULT NULL
                """)

        await cursor.execute("""
            SHOW COLUMNS FROM rooms LIKE 'room_type'
            """)
        result = await cursor.fetchone()
        if result:
            print(f"✅ Column room_type already exists in rooms.")
        else:
            print(f"⚙️ Adding column room_type to rooms...")
            await cursor.execute(f"""
                ALTER TABLE rooms
                ADD COLUMN room_type VARCHAR(16) NOT NULL DEFAULT 'group'
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

        # One client per (organization, username). dev_ccss used to create a second row when an employee
        # was saved from another organization's context; this index makes that impossible.
        await cursor.execute("SHOW INDEX FROM clients WHERE Key_name = 'clients_organization_username_unique'")
        result = await cursor.fetchone()
        if result:
            print("✅ Unique index on clients (organization_id, username) already exists.")
        else:
            await cursor.execute(
                """
                SELECT organization_id, username, COUNT(*)
                FROM clients
                WHERE username IS NOT NULL
                GROUP BY organization_id, username
                HAVING COUNT(*) > 1
                LIMIT 20
                """
            )
            duplicates = await cursor.fetchall()
            if duplicates:
                # Rooms and messages point at a client id, so merging needs a person; startup must not fail.
                print("⚠️ Unique index on clients (organization_id, username) not added; remove these duplicates first "
                      "(see dev_ccss docs/chat-client-duplicates.md): "
                      + ", ".join(f"{row[1]} (organization {row[0]})" for row in duplicates))
            else:
                try:
                    print("⚙️ Adding unique index on clients (organization_id, username)...")
                    # A prefix keeps the key within the index size limit of every row format.
                    await cursor.execute(
                        "CREATE UNIQUE INDEX clients_organization_username_unique ON clients (organization_id, username(191))"
                    )
                except Exception as error:
                    print(f"⚠️ Could not add unique index on clients (organization_id, username): {error}")

        if notify_user and notify_user_path:
            await cursor.execute(
                """
                SELECT id
                FROM clients
                WHERE username = %s AND token = %s AND organization_id = 0
                LIMIT 1
                """,
                (notify_user, notify_user_path),
            )
            result = await cursor.fetchone()
            if result:
                print("✅ Notify user already exists in clients.")
            else:
                # With the unique index, a changed NOTIFY_USER_PATH updates the existing row instead of failing.
                print("⚙️ Adding notify user to clients...")
                await cursor.execute(
                    """
                    INSERT INTO clients (username, token, organization_id)
                    VALUES (%s, %s, 0)
                    ON DUPLICATE KEY UPDATE token = VALUES(token)
                    """,
                    (notify_user, notify_user_path),
                )
        else:
            print("ℹ️ NOTIFY_USER or NOTIFY_USER_PATH missing in config; skipping notify user bootstrap.")

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
            device_tokens = notification_device_tokens(result, user_id, organization_id)
            if device_tokens is None:
                return
            invalid_tokens = send_device_notifications(
                device_tokens, user_id, organization_id, msg_title, msg_body, data
            )
            if invalid_tokens:
                notification_logger.warning(
                    "Removing unregistered device tokens: user_id=%s organization_id=%s count=%s",
                    user_id, organization_id, len(invalid_tokens),
                )
                device_tokens = [t for t in device_tokens if not isinstance(t, dict) or t.get("token") not in invalid_tokens]
                new_value = json.dumps(device_tokens) if device_tokens else None
                await cursor.execute(
                    "UPDATE clients SET device_token = %s WHERE id = %s AND organization_id = %s",
                    (new_value, user_id, int(organization_id)),
                )
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
            device_tokens = notification_device_tokens(result, user_id, organization_id)
            if device_tokens is None:
                return
            invalid_tokens = send_device_notifications(
                device_tokens, user_id, organization_id, msg_title, msg_body, data
            )
            if invalid_tokens:
                notification_logger.warning(
                    "Removing unregistered device tokens: user_id=%s organization_id=%s count=%s",
                    user_id, organization_id, len(invalid_tokens),
                )
                device_tokens = [t for t in device_tokens if not isinstance(t, dict) or t.get("token") not in invalid_tokens]
                new_value = json.dumps(device_tokens) if device_tokens else None
                await cursor.execute(
                    "UPDATE clients SET device_token = %s WHERE id = %s AND organization_id = %s",
                    (new_value, user_id, int(organization_id)),
                )
    
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
                SELECT r.id, r.name, r.description, r.room_type, r.last_message_at, ru.last_message_seen, r.owner_id, ru.silent_notifications
                FROM rooms r 
                JOIN room_participants ru ON ru.room_id = r.id
                WHERE ru.user_id = %s
                AND ru.deleted_at IS NULL
                ORDER BY r.last_message_at DESC, r.id DESC
            """, (user_id,))
            rooms = await cursor.fetchall()
            for room in rooms:
                for field, value in room.items():
                    if isinstance(value, datetime):
                        room[field] = value.isoformat()
            return rooms

async def store_new_message (pool, user_id, message, msginfo, room_id, organization_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("INSERT INTO room_messages (room_id, user_id, organization_id, message, message_information) VALUES( %s, %s, %s, %s, %s)", (room_id, user_id, int( organization_id), message, msginfo))       
            await cursor.execute("UPDATE rooms SET last_message_at = NOW() WHERE id = %s", (room_id,))
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
    batch_size = 250
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            for start in range(0, len(user_ids), batch_size):
                batch_user_ids = user_ids[start:start + batch_size]
                placeholders = ','.join(['%s'] * len(batch_user_ids))
                sql = f"""
                    UPDATE room_participants 
                    SET last_message_seen = {msg_id} - 1
                    WHERE room_id = %s 
                    AND user_id IN ({placeholders})
                """
                await cursor.execute(sql, (room_id, *batch_user_ids))
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

def normalize_room_type(room_type, participant_count):
    if isinstance(room_type, str):
        room_type = room_type.strip().lower()
    else:
        room_type = None

    aliases = {
        "dm": "direct",
        "direct_message": "direct",
        "direct": "direct",
        "group": "group",
    }
    mapped = aliases.get(room_type)
    if mapped:
        return mapped

    if participant_count == 2:
        return "direct"
    return "group"

async def get_user_id_in_organization(pool, user_id, organization_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SELECT id FROM clients WHERE id = %s AND organization_id = %s", (user_id, int(organization_id)))
            user = await cursor.fetchone()
            return user['id'] if user else None

async def resolve_user_ids(pool, raw_user_ids, creator_user_id, organization_id):
    resolved = set()
    for uid in raw_user_ids:
        resolved_uid = uid
        if not isinstance(uid, str) or uid.isdigit() == False:
            resolved_uid = await get_user_id(uid, organization_id)
        elif isinstance(uid, str):
            # Numeric IDs must belong to the creator's organization, like usernames.
            resolved_uid = await get_user_id_in_organization(pool, int(uid), organization_id)
        if resolved_uid is not None:
            resolved.add(int(resolved_uid))
    resolved.add(int(creator_user_id))
    return list(resolved)

async def find_existing_direct_room(pool, organization_id, user_a_id, user_b_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                SELECT r.id
                FROM rooms r
                JOIN room_participants rp ON rp.room_id = r.id
                WHERE r.organization_id = %s
                  AND r.room_type = 'direct'
                GROUP BY r.id
                HAVING COUNT(DISTINCT rp.user_id) = 2
                   AND SUM(CASE WHEN rp.user_id = %s THEN 1 ELSE 0 END) > 0
                   AND SUM(CASE WHEN rp.user_id = %s THEN 1 ELSE 0 END) > 0
                ORDER BY r.id DESC
                LIMIT 1
            """, (int(organization_id), int(user_a_id), int(user_b_id)))
            row = await cursor.fetchone()
            return row["id"] if row else None

async def ensure_room_participant(cursor, room_id, participant_id, organization_id):
    await cursor.execute("""
        SELECT id
        FROM room_participants
        WHERE room_id = %s
          AND user_id = %s
          AND organization_id = %s
          AND deleted_at IS NULL
        ORDER BY id DESC
        LIMIT 1
    """, (room_id, participant_id, int(organization_id)))
    existing_active = await cursor.fetchone()
    if existing_active:
        return

    await cursor.execute("""
        SELECT id
        FROM room_participants
        WHERE room_id = %s
          AND user_id = %s
          AND organization_id = %s
        ORDER BY id DESC
        LIMIT 1
    """, (room_id, participant_id, int(organization_id)))
    existing_any = await cursor.fetchone()
    if existing_any:
        await cursor.execute("""
            UPDATE room_participants
            SET deleted_at = NULL
            WHERE id = %s
        """, (existing_any[0] if not isinstance(existing_any, dict) else existing_any["id"],))
        return

    await cursor.execute("""
        INSERT INTO room_participants (room_id, user_id, last_message_seen, organization_id)
        VALUES (%s, %s, %s, %s)
    """, (room_id, participant_id, 0, int(organization_id)))

async def create_or_update_room(pool, user_id, room_name, user_ids, description, organization_id, requested_room_type=None):
    participant_ids = await resolve_user_ids(pool, user_ids, user_id, organization_id)
    room_type = normalize_room_type(requested_room_type, len(participant_ids))
    if isinstance(requested_room_type, str) and requested_room_type.strip().lower() in {"dm", "direct_message", "direct"} and len(participant_ids) != 2:
        return None, "direct", "Direct rooms require exactly two valid participants in the same organization"

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            if room_type == "direct" and len(participant_ids) == 2:
                direct_room_id = await find_existing_direct_room(pool, organization_id, participant_ids[0], participant_ids[1])
                if direct_room_id:
                    for uid in participant_ids:
                        await ensure_room_participant(cursor, direct_room_id, uid, organization_id)
                    await conn.commit()
                    return direct_room_id, room_type, None

            existing_room = None
            if room_type != "direct":
                await cursor.execute("SELECT id FROM rooms WHERE name = %s AND organization_id = %s", (room_name, organization_id))
                existing_room = await cursor.fetchone()

            if existing_room:
                room_id = existing_room[0]
                if await is_user_room_owner(pool, user_id, room_id, organization_id) == False:
                    return None, room_type, "Only the room owner can update this room"
                await cursor.execute(
                    "UPDATE rooms SET description = %s, name = %s, room_type = %s WHERE id = %s",
                    (description, room_name, room_type, room_id),
                )
                await cursor.execute("DELETE FROM room_participants WHERE room_id = %s", (room_id,))
            else:
                if organization_id:
                    await cursor.execute(
                        "INSERT INTO rooms (name, organization_id, description, owner_id, room_type) VALUES (%s, %s, %s, %s, %s)",
                        (room_name, int(organization_id), description, user_id, room_type),
                    )
                else:
                    await cursor.execute(
                        "INSERT INTO rooms (name, description, owner_id, room_type) VALUES (%s, %s, %s, %s)",
                        (room_name, description, user_id, room_type),
                    )
                room_id = cursor.lastrowid

            for uid in participant_ids:
                await cursor.execute(
                    "INSERT INTO room_participants (room_id, user_id, last_message_seen, organization_id) VALUES (%s, %s, %s, %s)",
                    (room_id, uid, 0, int(organization_id)),
                )

            await conn.commit()
            return room_id, room_type, None

def isUserOnline( user_id ):
    for ws, info in connected_clients.items():
        if info.get("user_id") == user_id :
            return True
    return False

async def send_general_notification_msg_to_users( pool, message, user_id, organization_id, msg_title, msg_body ):
    tasks = []
    found = False
    notification_logger.info("Routing notification: user_id=%s organization_id=%s", user_id, organization_id)
    for ws, info in connected_clients.items():
        ws_user_id = info.get("user_id")
        if(user_id == ws_user_id) :
            found = True
            tasks.append(ws.send(message))
            break

    if found == False:
        notification_logger.info("Recipient offline: routing to Firebase user_id=%s organization_id=%s", user_id, organization_id)
        await send_general_notifcation_message( pool, user_id, organization_id, msg_title, msg_body, message ) 
    if tasks:
        notification_logger.info(
            "Firebase skipped: recipient has an active WebSocket user_id=%s organization_id=%s",
            user_id, organization_id,
        )
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                notification_logger.error(
                    "WebSocket notification failed: user_id=%s organization_id=%s error_type=%s error=%s",
                    user_id, organization_id, type(result).__name__, result,
                )
            else:
                notification_logger.info("WebSocket notification sent: user_id=%s organization_id=%s", user_id, organization_id)

async def send_msg_to_users( pool, message, user_ids, organization_id, room_id ):
    tasks = []
    for user_id in user_ids:
        found = False
        for ws, info in connected_clients.items():
            ws_user_id = info.get("user_id")
            # Never deliver to a participant from another organization.
            if(user_id == ws_user_id and info.get("organization_id") == organization_id) :
                found = True
                tasks.append(ws.send(message))
                break
        if found == False:
            if await can_send_message(pool, user_id, organization_id, room_id ):
                await send_notifcation_message( pool, user_id, organization_id, "New Message", "A new chat message is sent to you", room_id )
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# WebSocket server
async def send_call_notification(websocket, payload):
    try:
        status, reply = await call_notification_dispatcher.dispatch(payload)
        # Replies can arrive out of order; call_id lets the sender match them.
        reply = dict({"call_id": payload.get("call_id")}, **reply)
        reply["event"] = "call_notification_success" if status == 200 else "call_notification_failed"
        await websocket.send(json.dumps(reply))
    except websockets.ConnectionClosed:
        pass
    except Exception:
        notification_logger.exception("Call notification reply failed: call_id=%s", payload.get("call_id"))


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

                if event == "call_notification":
                    data = theMessageContent.get("data") or {}
                    # Encoded so a non-ASCII token is rejected rather than raising.
                    if not isinstance(data, dict) or not secrets.compare_digest(
                        str(data.get("session_token", "")).encode(), str(client_info["session_token"]).encode()
                    ):
                        await websocket.send(json.dumps({"event": "call_notification_failed", "error": "invalid_session"}))
                        continue
                    # Only the shared notification account (organization 0) may
                    # raise call alerts; ordinary chat users cannot.
                    if int(client_info.get("organization_id") or 0) != 0:
                        notification_logger.warning("Call notification rejected: sender is not the notification account sender_user_id=%s", client_info.get("user_id"))
                        await websocket.send(json.dumps({"event": "call_notification_failed", "error": "recipient_not_authorized"}))
                        continue
                    organization_id = theMessageContent.get("organization_id")
                    try:
                        # Same coercion as the notification event, which accepts string IDs.
                        organization_id = None if isinstance(organization_id, bool) else int(organization_id)
                    except (TypeError, ValueError):
                        organization_id = None
                    payload = {
                        "username": theMessageContent.get("username"),
                        "organization_id": organization_id,
                        **{key: data.get(key) for key in ("call_id", "caller_name", "caller_number", "extension")},
                    }
                    # Dispatched in the background so a slow push does not hold up
                    # later alerts on the shared notification connection.
                    task = asyncio.create_task(send_call_notification(websocket, payload))
                    call_notification_tasks.add(task)
                    task.add_done_callback(call_notification_tasks.discard)
                    continue

                ## send notifications to clients
                if event == "notification":
                    notification_logger.info(
                        "Notification request received: sender_user_id=%s sender_organization_id=%s target_organization_id=%s",
                        client_info.get("user_id"), client_info.get("organization_id"), theMessageContent.get("organization_id"),
                    )
                    data = theMessageContent.get("data") or {}
                    session_token = data.get('session_token')
                    if client_info['session_token'] != session_token :
                        notification_logger.warning("Notification rejected: invalid session token sender_user_id=%s", client_info.get("user_id"))
                        print(f"{client_ip}: invalid token Session token is invalid")
                        await websocket.send(json.dumps({
                            "error":"invalid token",
                            "data":"Session token is invalid"
                        }))
                        continue

                    organization_id = theMessageContent.get("organization_id")
                    if organization_id is None:
                        notification_logger.warning("Notification rejected: missing organization_id")
                        print(f"{client_ip}: invalid organization id")
                        await websocket.send(json.dumps({
                            "error":"invalid organization id",
                            "data":"organization id is missing"
                        }))
                        continue

                    org_id = client_info['organization_id']
                    if int(org_id) > 0 and int(org_id) != int(organization_id) :
                        notification_logger.warning("Notification rejected: organization mismatch sender_organization_id=%s target_organization_id=%s", org_id, organization_id)
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
                                "message": payload,
                            }
                        })
                        await send_general_notification_msg_to_users(pool, notification_message, user_id, organization_id, msg_title, msg_body)
                        await websocket.send( json.dumps({
                            "event":"notification_success",
                            }))
                    else :
                        notification_logger.warning("Notification rejected: target username not found organization_id=%s", organization_id)
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

                ## create a rooms param: session_token, name, users, description, type(optional: group/direct)
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
                    requested_room_type = data.get("type")
                    org_id = client_info['organization_id']
                    room_id, room_type, error_message = await create_or_update_room(
                        pool, user_id, room_name, user_names, description, org_id, requested_room_type
                    )
                    if room_id == None: 
                        await websocket.send(json.dumps({
                            "event":"update_or_make_room",
                            "data":{ 
                                "room": room_id,
                                "type": room_type,
                                "status": "failed",
                                "msg": error_message or "Failed to create a room"
                                }
                            }))
                    else:
                        await websocket.send(json.dumps({
                            "event":"update_or_make_room",
                            "data":{
                                "room": room_id,
                                "name": room_name,
                                "type": room_type,
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
                notification_logger.exception("WebSocket request failed: event=%s sender_user_id=%s", theMessageContent.get("event"), connected_clients.get(websocket, {}).get("user_id"))
                print(f"WebSocket error: {outer_err}")
    except websockets.ConnectionClosed:
        pass
    finally:
        connected_clients.pop(websocket, None)

def send_push_notification(token, title, body, data=None, user_id=None, organization_id=None):
    """
    Sends a push notification to a specific device via FCM.
    :param token: The FCM device registration token.
    :param title: Notification title.
    :param body: Notification body.
    :param data: Optional custom key/value payload (dict).
    """
    context = "user_id=%s organization_id=%s token_fingerprint=%s"
    context_args = (user_id, organization_id, device_token_fingerprint(token))
    try:
        app = firebase_admin.get_app()
        notification_logger.info(
            "FCM send attempt: " + context + " project_id=%s type=%s title_present=%s body_present=%s data_bytes=%s",
            *context_args, app.project_id, (data or {}).get("type"), bool(title), bool(body),
            len(json.dumps(data or {}).encode()),
        )
        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            token=token,
            data=data or {},
        )
        if (data or {}).get("type") == "incoming_call":
            # A short-lived ordinary alert, not a PushKit VoIP push.
            message.android = messaging.AndroidConfig(priority="high", ttl=timedelta(seconds=30))
            message.apns = messaging.APNSConfig(
                headers={"apns-push-type": "alert", "apns-priority": "10",
                         "apns-expiration": str(int(datetime.now().timestamp()) + 30)},
                payload=messaging.APNSPayload(aps=messaging.Aps(sound="default")),
            )
        response = messaging.send(message)
        notification_logger.info("FCM accepted (device display not confirmed): " + context + " message_id=%s", *context_args, response)
        return "ok"
    except messaging.UnregisteredError as e:
        notification_logger.exception("FCM token unregistered: " + context + " code=%s", *context_args, getattr(e, "code", None))
        return "unregistered"
    except exceptions.FirebaseError as e:
        notification_logger.exception("FCM send failed: " + context + " code=%s", *context_args, getattr(e, "code", None))
        return "error"
    except Exception as e:
        notification_logger.exception("FCM unexpected failure: " + context + " error_type=%s", *context_args, type(e).__name__)
        return "error"


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
    global pool, call_notification_dispatcher

    try:
        notification_logger.info("Firebase initialization: credentials_path=%s database=%s", os.path.abspath("firebase_credentials.json"), DB_NAME)
        cred = credentials.Certificate("firebase_credentials.json")
        app = firebase_admin.initialize_app(cred)
        notification_logger.info("Firebase initialized: project_id=%s", app.project_id)

    except ValueError as e:
        # ValueError can also indicate invalid credentials, not just an existing app.
        notification_logger.exception("Firebase initialization ValueError")
        try:
            notification_logger.info("Existing Firebase app: project_id=%s", firebase_admin.get_app().project_id)
        except ValueError:
            notification_logger.error("Firebase unavailable: no initialized app; push attempts will fail")

    except FileNotFoundError as e:
        # Happens if the service account file path is wrong
        notification_logger.exception("Firebase credential file not found")

    except exceptions.FirebaseError as e:
        # Catches Firebase-specific errors (e.g., invalid credentials)
        notification_logger.exception("Firebase initialization error: code=%s", getattr(e, "code", None))

    except Exception as e:
        # Generic catch-all for anything unexpected
        notification_logger.exception("Firebase unexpected initialization failure")

    await init_db()
    pool = await create_pool()
    call_notification_dispatcher = CallNotificationDispatcher(
        pool, notification_device_tokens, send_push_notification, notification_logger,
    )

    # Start WebSocket server on port 8080
    #ws_server = await websockets.serve(lambda ws, path:ws_handler(ws, path, pool), SERVER_IP, 8080)
    ws_server = await websockets.serve(ws_handler, SERVER_IP, SERVER_PORT)

    runner = None
    pbx_secret = getattr(config, "PBX_NOTIFICATION_SECRET", "")
    if pbx_secret:
        pbx_host = getattr(config, "PBX_NOTIFICATION_HOST", "127.0.0.1")
        pbx_port = getattr(config, "PBX_NOTIFICATION_PORT", 8081)
        # Shares the WebSocket dispatcher so deduplication covers both transports.
        runner = web.AppRunner(create_pbx_app(call_notification_dispatcher, pbx_secret, notification_logger))
        try:
            await runner.setup()
            await web.TCPSite(runner, pbx_host, pbx_port).start()
            notification_logger.info("PBX notifications listening: host=%s port=%s", pbx_host, pbx_port)
        except OSError:
            # The fallback is optional; chat and WebSocket alerts keep running without it.
            notification_logger.exception("PBX HTTP fallback failed to start: host=%s port=%s", pbx_host, pbx_port)
            await runner.cleanup()
            runner = None
    else:
        notification_logger.info("PBX HTTP fallback disabled: PBX_NOTIFICATION_SECRET is not configured; WebSocket call notifications enabled")

    chat_runner = None
    # No default port: instances sharing a server each need their own.
    chat_port = getattr(config, "CHAT_HTTP_PORT", None)
    if chat_port:
        # The API has no credentials; keep it on localhost unless nginx restricts access.
        chat_host = getattr(config, "CHAT_HTTP_HOST", "127.0.0.1")
        # The same operations the WebSocket events use, so both transports behave alike.
        chat_operations = SimpleNamespace(
            get_user_rooms=get_user_rooms, create_or_update_room=create_or_update_room,
            get_room_owner=get_room_owner, get_user_names_in_room=get_user_names_in_room,
            leave_room=leave_room, silent_room=silent_room, unsilent_room=unsilent_room,
            update_last_seen_msg_in_room=update_last_seen_msg_in_room, clear_user_last_seen_msg=clear_user_last_seen_msg,
            get_prev_messages_in_room=get_prev_messages_in_room, get_last_messages_in_room=get_last_messages_in_room,
            get_users_in_room=get_users_in_room, store_new_message=store_new_message,
            edit_message_in_room=edit_message_in_room, delete_message_in_room=delete_message_in_room,
            send_msg_to_users=send_msg_to_users, is_user_online=isUserOnline,
            send_general_notification_msg_to_users=send_general_notification_msg_to_users,
        )
        chat_runner = web.AppRunner(create_chat_app(pool, notification_logger, chat_operations))
        try:
            await chat_runner.setup()
            await web.TCPSite(chat_runner, chat_host, chat_port).start()
            notification_logger.info("Chat HTTP polling listening: host=%s port=%s", chat_host, chat_port)
        except OSError:
            # Optional like the PBX fallback; WebSocket chat keeps running without it.
            notification_logger.exception("Chat HTTP polling failed to start: host=%s port=%s", chat_host, chat_port)
            await chat_runner.cleanup()
            chat_runner = None
    else:
        notification_logger.info("Chat HTTP polling disabled: CHAT_HTTP_PORT is not configured")

    print(f"WebSocket: ws://{SERVER_IP}:{SERVER_PORT}")
    #print("HTTP POST: http://{SERVER_IP}:8081/sendmessage")
    try:
        await asyncio.Future()  # run forever
    finally:
        if runner is not None:
            await runner.cleanup()
        if chat_runner is not None:
            await chat_runner.cleanup()
        ws_server.close()
        await ws_server.wait_closed()


asyncio.run(main())
