"""PBX call alerts over HTTP or authenticated WebSocket sessions."""

import asyncio
import hmac
import json
import time
import uuid

import aiomysql
from aiohttp import web


class CallNotificationDispatcher:
    def __init__(self, pool, read_tokens, send_push, logger):
        self.pool = pool
        self.read_tokens = read_tokens
        self.send_push = send_push
        self.logger = logger
        self.completed = {}
        self.in_progress = set()

    async def dispatch(self, payload):
        """Send one call alert. Returns (status, body) using HTTP status semantics."""
        if not isinstance(payload, dict):
            return 400, {"error": "expected_json_object"}
        for field in ("organization_id",):
            if type(payload.get(field)) is not int or payload[field] <= 0:
                return 400, {"error": "invalid_" + field}
        fields = {
            "call_id": (128, True), "caller_name": (128, False),
            "caller_number": (64, False), "extension": (32, False),
        }
        values = {}
        for field, (limit, required) in fields.items():
            value = payload.get(field)
            if value is None and not required:
                # Senders encode a missing caller ID as JSON null.
                value = ""
            if not isinstance(value, str) or len(value) > limit or (required and not value.strip()):
                return 400, {"error": "invalid_" + field}
            values[field] = value

        user_id = payload.get("user_id")
        username = payload.get("username")
        if user_id is not None:
            if type(user_id) is not int or user_id <= 0 or username is not None:
                return 400, {"error": "invalid_recipient"}
        elif not isinstance(username, str) or not username.strip() or len(username) > 512:
            return 400, {"error": "invalid_username"}
        organization_id = payload["organization_id"]
        call_id = values["call_id"]
        key = None
        self.logger.info(
            "PBX call notification: user_id=%s organization_id=%s call_id=%s", user_id, organization_id, call_id,
        )
        try:
            # Release the connection before making external Firebase requests.
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    column = "id" if user_id is not None else "username"
                    await cursor.execute(
                        f"SELECT id, device_token FROM clients WHERE {column} = %s AND organization_id = %s",
                        (user_id if user_id is not None else username, organization_id),
                    )
                    row = await cursor.fetchone()
            if not row:
                return 404, {"error": "user_not_found"}
            user_id = row.get("id", user_id)

            # Keyed by the resolved user so user-ID and username requests for
            # the same call share one deduplication entry.
            candidate = (organization_id, user_id, call_id)
            now = time.monotonic()
            for old_key, entry in list(self.completed.items()):
                if entry[0] <= now:
                    self.completed.pop(old_key)
            if candidate in self.in_progress:
                return 409, {"error": "notification_in_progress"}
            self.in_progress.add(candidate)
            key = candidate

            tokens = self.read_tokens(row, user_id, organization_id)
            if not tokens:
                return 409, {"error": "no_device_tokens"}
            cached = self.completed.get(key)
            delivered = cached[2] if cached else set()
            counts = {"accepted": 0, "unregistered": 0, "failed": 0}
            # Each device entry also says how the app wants calls delivered (see send_push).
            pending = {}
            for entry in tokens:
                token = entry.get("token") if isinstance(entry, dict) else None
                if not isinstance(token, str) or not token.strip():
                    counts["failed"] += 1
                elif token not in delivered and token not in pending:
                    pending[token] = entry
            if cached and not pending:
                return 200, dict(cached[1], duplicate=True)

            caller = values["caller_name"] or values["caller_number"] or "Unknown caller"
            data = {
                "type": "incoming_call", "action": "open_calls", "call_id": call_id,
                "caller": caller, "caller_name": values["caller_name"],
                "caller_number": values["caller_number"], "extension": values["extension"],
                # One UUID per call, so CallKit and the Android call screen show a repeated push as one call.
                "callkit_id": str(uuid.uuid5(uuid.NAMESPACE_URL, "ccss-call:" + call_id)),
            }
            # Always push for PBX alerts, including users with an active WebSocket.
            # Devices are sent concurrently so one slow token does not delay the rest.
            statuses = await asyncio.gather(*(
                asyncio.to_thread(self.send_push, token, "Incoming call", caller, data, user_id, organization_id,
                                  device=entry)
                for token, entry in pending.items()
            ))
            accepted = set()
            unregistered = set()
            voip_gone = set()
            for token, status in zip(pending, statuses):
                if status in ("ok", "voip_unregistered"):
                    counts["accepted"] += 1
                    accepted.add(token)
                    if status == "voip_unregistered":
                        voip_gone.add(token)
                elif status == "unregistered":
                    counts["unregistered"] += 1
                    unregistered.add(token)
                else:
                    counts["failed"] += 1
            self.logger.info(
                "PBX push result: user_id=%s organization_id=%s call_id=%s accepted=%s unregistered=%s failed=%s",
                user_id, organization_id, call_id, counts["accepted"], counts["unregistered"], counts["failed"],
            )
            if unregistered or voip_gone:
                await self.remove_tokens(tokens, unregistered, user_id, organization_id, voip_gone)
            response = dict(counts, call_id=call_id, status="accepted" if counts["accepted"] else "failed")
            if counts["accepted"]:
                # Bounded process-local deduplication; not a durable delivery receipt.
                # Only accepted tokens are remembered, so a retry of the same call
                # still reaches devices that failed.
                if key not in self.completed and len(self.completed) >= 10000:
                    self.completed.pop(next(iter(self.completed)))
                self.completed[key] = (time.monotonic() + 60, response, delivered | accepted)
            return (200 if counts["accepted"] else 502), response
        except Exception:
            self.logger.exception("PBX notification failed: user_id=%s organization_id=%s call_id=%s", user_id, organization_id, call_id)
            return 500, {"error": "notification_failed"}
        finally:
            if key is not None:
                self.in_progress.discard(key)

    async def remove_tokens(self, tokens, invalid_tokens, user_id, organization_id, voip_gone=()):
        """Removes devices Firebase no longer knows, and VoIP tokens APNs refused from devices that remain."""
        self.logger.warning(
            "Removing unregistered device tokens: user_id=%s organization_id=%s count=%s voip_count=%s",
            user_id, organization_id, len(invalid_tokens), len(voip_gone),
        )
        remaining = []
        for entry in tokens:
            if isinstance(entry, dict) and entry.get("token") in invalid_tokens:
                continue
            if isinstance(entry, dict) and entry.get("token") in voip_gone:
                # The device keeps its ordinary alerts until the app registers a new VoIP token.
                entry = {key: value for key, value in entry.items() if key != "voip_token"}
            remaining.append(entry)
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        "UPDATE clients SET device_token = %s WHERE id = %s AND organization_id = %s",
                        (json.dumps(remaining) if remaining else None, user_id, organization_id),
                    )
        except Exception:
            # The alert itself was already sent; pruning is retried on the next push.
            self.logger.exception("Removing unregistered device tokens failed: user_id=%s organization_id=%s", user_id, organization_id)


def create_pbx_app(dispatcher, secret, logger):
    async def notify(request):
        authorization = request.headers.get("Authorization", "")
        if not secret or not hmac.compare_digest(
            authorization.encode(), ("Bearer " + secret).encode()
        ):
            logger.warning("PBX notification rejected: invalid service authentication")
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            payload = await request.json()
        except (ValueError, UnicodeDecodeError):
            return web.json_response({"error": "invalid_json"}, status=400)
        status, body = await dispatcher.dispatch(payload)
        return web.json_response(body, status=status)

    app = web.Application(client_max_size=8192)
    app.router.add_post("/pbx/notifications", notify)
    return app
