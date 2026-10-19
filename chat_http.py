"""HTTP version of the WebSocket chat events, for servers that cannot hold a WebSocket open.

A trusted service authenticates with a shared secret and acts as a chat user named by
username and organization_id. Chat operations come from main.py through `chat`, so HTTP
messages are stored, relayed to WebSocket sessions and pushed exactly like WebSocket ones.
"""

import hmac
import json
from datetime import datetime
from types import SimpleNamespace

import aiomysql
from aiohttp import web

DEFAULT_LIMIT = 20
MAX_LIMIT = 100


def _serialize(row):
    return {field: value.isoformat() if isinstance(value, datetime) else value for field, value in row.items()}


def _positive_int(value, allow_zero=False):
    # Query strings carry text and JSON bodies carry numbers; reject signs, decimals, blanks and booleans.
    if isinstance(value, int) and not isinstance(value, bool):
        number = value
    elif isinstance(value, str) and value.isdigit():
        number = int(value)
    else:
        return None
    if number < 0 or (number == 0 and not allow_zero):
        return None
    return number


def _error(status, code, **extra):
    return web.json_response(dict({"error": code}, **extra), status=status)


async def find_user(pool, username, organization_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute(
                "SELECT id, username FROM clients WHERE username = %s AND organization_id = %s LIMIT 1",
                (username, organization_id),
            )
            return await cursor.fetchone()


async def get_unread_summary(pool, user_id, organization_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            # The user's own messages never count as unread.
            await cursor.execute("""
                SELECT r.id AS room, r.name, r.room_type, r.last_message_at,
                       COALESCE(rp.last_message_seen, 0) AS last_message_seen,
                       (SELECT MAX(m.id) FROM room_messages m
                         WHERE m.room_id = r.id AND m.organization_id = rp.organization_id
                           AND m.is_deleted = 0) AS last_message_id,
                       (SELECT COUNT(*) FROM room_messages m
                         WHERE m.room_id = r.id AND m.organization_id = rp.organization_id
                           AND m.is_deleted = 0 AND m.user_id <> rp.user_id
                           AND m.id > COALESCE(rp.last_message_seen, 0)) AS unread_count
                FROM room_participants rp
                JOIN rooms r ON r.id = rp.room_id
                WHERE rp.user_id = %s
                  AND rp.organization_id = %s
                  AND rp.deleted_at IS NULL
                ORDER BY r.last_message_at DESC, r.id DESC
            """, (user_id, organization_id))
            rows = await cursor.fetchall()
    return [_serialize(row) for row in rows]


async def get_participant(pool, user_id, room_id, organization_id):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("""
                SELECT COALESCE(last_message_seen, 0) AS last_message_seen
                FROM room_participants
                WHERE user_id = %s AND room_id = %s AND organization_id = %s AND deleted_at IS NULL
                ORDER BY id DESC
                LIMIT 1
            """, (user_id, room_id, organization_id))
            return await cursor.fetchone()


async def get_new_messages(pool, room_id, organization_id, last_id, limit):
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            # One extra row tells the caller whether to poll again straight away.
            await cursor.execute("""
                SELECT m.id, m.user_id, u.username, m.room_id, m.message, m.message_information, m.created_at, m.updated_at
                FROM room_messages m
                JOIN clients u ON m.user_id = u.id
                WHERE m.room_id = %s
                  AND m.organization_id = %s
                  AND m.is_deleted = 0
                  AND m.id > %s
                ORDER BY m.id ASC
                LIMIT %s
            """, (room_id, organization_id, last_id, limit + 1))
            rows = await cursor.fetchall()
    return [_serialize(row) for row in rows[:limit]], len(rows) > limit


def _message_fields(params):
    """Returns (message, msginfo, None) or (None, None, error_response)."""
    message = params.get("message")
    msginfo = params.get("msginfo")
    if not isinstance(message, str):
        return None, None, _error(400, "invalid_message")
    if msginfo is not None and not isinstance(msginfo, str):
        return None, None, _error(400, "invalid_msginfo")
    return message, msginfo, None


def create_chat_app(pool, secret, logger, chat):
    def endpoint(handler, room_scoped=False):
        async def wrapped(request):
            authorization = request.headers.get("Authorization", "")
            if not secret or not hmac.compare_digest(authorization.encode(), ("Bearer " + secret).encode()):
                logger.warning("Chat HTTP request rejected: invalid service authentication path=%s", request.path)
                return _error(401, "unauthorized")

            params = dict(request.query)
            if request.method == "POST" and request.can_read_body:
                try:
                    body = await request.json()
                except (ValueError, UnicodeDecodeError):
                    return _error(400, "invalid_json")
                if not isinstance(body, dict):
                    return _error(400, "expected_json_object")
                params.update(body)

            organization_id = _positive_int(params.get("organization_id"))
            if organization_id is None:
                return _error(400, "invalid_organization_id")
            username = params.get("username")
            if not isinstance(username, str) or not username.strip() or len(username) > 512:
                return _error(400, "invalid_username")

            try:
                user = await find_user(pool, username, organization_id)
                if not user:
                    return _error(404, "user_not_found")
                ctx = SimpleNamespace(user=user, user_id=user["id"], organization_id=organization_id,
                                      params=params, room_id=None, participant=None)
                if room_scoped:
                    ctx.room_id = _positive_int(request.match_info.get("room", params.get("room")))
                    if ctx.room_id is None:
                        return _error(400, "invalid_room")
                    # Unlike the WebSocket events, only active participants may use a room.
                    ctx.participant = await get_participant(pool, ctx.user_id, ctx.room_id, organization_id)
                    if not ctx.participant:
                        return _error(403, "not_a_participant")
                return await handler(request, ctx)
            except Exception:
                logger.exception("Chat HTTP request failed: path=%s organization_id=%s", request.path, organization_id)
                return _error(500, "request_failed")
        return wrapped

    # Polling

    async def unread(request, ctx):
        rooms = await get_unread_summary(pool, ctx.user_id, ctx.organization_id)
        total = sum(int(room["unread_count"] or 0) for room in rooms)
        return web.json_response({
            "user_id": ctx.user_id,
            "has_new_messages": total > 0,
            "total_unread": total,
            "rooms": rooms,
        })

    # GetMessagesInRoom
    async def messages(request, ctx):
        limit = ctx.params.get("limit")
        limit = DEFAULT_LIMIT if limit is None else _positive_int(limit)
        if limit is None:
            return _error(400, "invalid_limit")
        limit = min(limit, MAX_LIMIT)
        last_id = ctx.params.get("last_id")
        if last_id is None:
            last_id = int(ctx.participant["last_message_seen"] or 0)
        else:
            last_id = _positive_int(last_id, allow_zero=True)
            if last_id is None:
                return _error(400, "invalid_last_id")
        msgs, has_more = await get_new_messages(pool, ctx.room_id, ctx.organization_id, last_id, limit)
        return web.json_response({
            "room": ctx.room_id,
            "last_id": last_id,
            "next_last_id": msgs[-1]["id"] if msgs else last_id,
            "has_more": has_more,
            "messages": msgs,
        })

    # GetPrevMessagesInRoom
    async def previous_messages(request, ctx):
        before_id = _positive_int(ctx.params.get("before_id"))
        if before_id is None:
            return _error(400, "invalid_before_id")
        msgs = await chat.get_prev_messages_in_room(pool, ctx.user_id, ctx.room_id, ctx.organization_id, before_id)
        return web.json_response({"room": ctx.room_id, "messages": msgs})

    # GetLastMessagesInRoom
    async def latest_messages(request, ctx):
        msgs = await chat.get_last_messages_in_room(pool, ctx.user_id, ctx.room_id, ctx.organization_id)
        return web.json_response({"room": ctx.room_id, "messages": msgs})

    # Session

    # Ping
    async def ping(request, ctx):
        return web.json_response({"status": True, "user_id": ctx.user_id})

    # GetUserStatus: online means the user has a WebSocket session open.
    async def user_status(request, ctx):
        return web.json_response({"user_id": ctx.user_id, "online": chat.is_user_online(ctx.user_id)})

    # notification: username/organization_id name the recipient.
    async def notification(request, ctx):
        data = ctx.params.get("data") or {}
        title = ctx.params.get("title")
        body = ctx.params.get("body")
        if isinstance(data, dict) and "notification" in data:
            payload = data["notification"]
        else:
            payload = data if isinstance(data, str) else json.dumps(data)
        message = json.dumps({
            "event": "notification",
            "data": {"title": title, "body": body, "message": payload},
        })
        await chat.send_general_notification_msg_to_users(pool, message, ctx.user_id, ctx.organization_id, title, body)
        return web.json_response({"success": True})

    # Rooms

    # GetRooms
    async def rooms(request, ctx):
        return web.json_response({"rooms": await chat.get_user_rooms(pool, ctx.user_id) or []})

    # UpdateOrMakeRoom
    async def make_room(request, ctx):
        name = ctx.params.get("name")
        users = ctx.params.get("users")
        description = ctx.params.get("description")
        room_type = ctx.params.get("type")
        if not isinstance(name, str) or not name.strip():
            return _error(400, "invalid_name")
        if not isinstance(users, list) or not all(isinstance(u, (str, int)) and not isinstance(u, bool) for u in users):
            return _error(400, "invalid_users")
        if description is not None and not isinstance(description, str):
            return _error(400, "invalid_description")
        if room_type is not None and not isinstance(room_type, str):
            return _error(400, "invalid_type")
        room_id, room_type, error_message = await chat.create_or_update_room(
            pool, ctx.user_id, name, users, description, ctx.organization_id, room_type
        )
        if room_id is None:
            return web.json_response({"status": "failed", "type": room_type,
                                      "error": error_message or "Failed to create a room"}, status=422)
        return web.json_response({"status": "success", "room": room_id, "name": name, "type": room_type})

    # GetUsersInRoom
    async def room_users(request, ctx):
        owners = await chat.get_room_owner(pool, ctx.room_id)
        users = await chat.get_user_names_in_room(pool, ctx.room_id)
        return web.json_response({"room": ctx.room_id, "users": users or [], "owners": owners or []})

    # LeaveRoom / SilentRoom / UnSilentRoom
    def room_toggle(operation):
        async def handler(request, ctx):
            return web.json_response({"success": bool(await operation(pool, ctx.room_id, ctx.user_id))})
        return handler

    # LastSeenMsg
    async def last_seen(request, ctx):
        msg_id = _positive_int(ctx.params.get("msg_id"), allow_zero=True)
        if msg_id is None:
            return _error(400, "invalid_msg_id")
        result = await chat.update_last_seen_msg_in_room(pool, ctx.user_id, ctx.room_id, msg_id, ctx.organization_id)
        return web.json_response({"success": bool(result)})

    # ClearLastMessageSeen
    async def clear_last_seen(request, ctx):
        await chat.clear_user_last_seen_msg(pool, ctx.user_id, ctx.room_id)
        return web.json_response({"success": True})

    # Messages

    # BroadcastMessage: the sender's own WebSocket sessions are skipped, as over WebSocket.
    async def send_message(request, ctx):
        message, msginfo, error = _message_fields(ctx.params)
        if error:
            return error
        user_ids = [uid for uid in await chat.get_users_in_room(pool, ctx.room_id) if uid != ctx.user_id]
        msg_id = await chat.store_new_message(pool, ctx.user_id, message, msginfo, ctx.room_id, ctx.organization_id)
        broadcast_data = json.dumps({
            "event": "chat_message",
            "data": {
                "username": ctx.user["username"],
                "msgid": msg_id,
                "room": ctx.room_id,
                "message": message,
                "msginfo": msginfo,
            }
        })
        await chat.send_msg_to_users(pool, broadcast_data, user_ids, ctx.organization_id, ctx.room_id)
        return web.json_response({"success": True, "msgid": msg_id})

    # EditMessageInRoom: only the author's own messages; unchanged text also reports false, as over WebSocket.
    async def edit_message(request, ctx):
        msg_id = _positive_int(request.match_info["msg_id"])
        if msg_id is None:
            return _error(400, "invalid_msg_id")
        message, msginfo, error = _message_fields(ctx.params)
        if error:
            return error
        result = await chat.edit_message_in_room(pool, ctx.user_id, msg_id, message, msginfo, ctx.room_id, ctx.organization_id)
        if result > 0:
            user_ids = await chat.get_users_in_room(pool, ctx.room_id)
            broadcast_data = json.dumps({
                "event": "chat_message_updated",
                "data": {
                    "username": ctx.user["username"],
                    "msgid": msg_id,
                    "room": ctx.room_id,
                    "message": message,
                    "msginfo": msginfo,
                }
            })
            await chat.send_msg_to_users(pool, broadcast_data, user_ids, ctx.organization_id, ctx.room_id)
        return web.json_response({"success": result > 0, "msgid": msg_id})

    # DeleteMessageInRoom: only the author's own messages.
    async def delete_message(request, ctx):
        msg_id = _positive_int(request.match_info["msg_id"])
        if msg_id is None:
            return _error(400, "invalid_msg_id")
        result = await chat.delete_message_in_room(pool, ctx.user_id, ctx.room_id, msg_id, ctx.organization_id)
        return web.json_response({"success": bool(result), "msgid": msg_id})

    app = web.Application(client_max_size=1024 * 1024)
    routes = [
        ("GET", "/chat/unread", unread, False),
        ("GET", "/chat/messages", messages, True),
        ("GET", "/chat/ping", ping, False),
        ("GET", "/chat/user-status", user_status, False),
        ("POST", "/chat/notifications", notification, False),
        ("GET", "/chat/rooms", rooms, False),
        ("POST", "/chat/rooms", make_room, False),
        ("GET", "/chat/rooms/{room}/users", room_users, True),
        ("POST", "/chat/rooms/{room}/leave", room_toggle(chat.leave_room), True),
        ("POST", "/chat/rooms/{room}/silent", room_toggle(chat.silent_room), True),
        ("POST", "/chat/rooms/{room}/unsilent", room_toggle(chat.unsilent_room), True),
        ("POST", "/chat/rooms/{room}/last-seen", last_seen, True),
        ("POST", "/chat/rooms/{room}/clear-last-seen", clear_last_seen, True),
        ("GET", "/chat/rooms/{room}/messages", messages, True),
        ("GET", "/chat/rooms/{room}/messages/previous", previous_messages, True),
        ("GET", "/chat/rooms/{room}/messages/latest", latest_messages, True),
        ("POST", "/chat/rooms/{room}/messages", send_message, True),
        ("POST", "/chat/rooms/{room}/messages/{msg_id}/edit", edit_message, True),
        ("POST", "/chat/rooms/{room}/messages/{msg_id}/delete", delete_message, True),
    ]
    for method, path, handler, room_scoped in routes:
        app.router.add_route(method, path, endpoint(handler, room_scoped))
    return app
