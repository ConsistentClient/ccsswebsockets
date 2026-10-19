# Chat HTTP API

Every WebSocket chat event has an HTTP equivalent, so a server such as `dev_ccss` can run chat with plain GET/POST requests. The mobile app keeps using WebSockets, and both work on the same data: a message sent over HTTP is stored, delivered to recipients' open WebSocket sessions, and pushed to offline recipients through Firebase, exactly like a WebSocket `BroadcastMessage`.

## Enable

Add to `config.py` and restart the service:

```python
CHAT_HTTP_PORT = 8082
CHAT_HTTP_HOST = "127.0.0.1"   # default
```

With no `CHAT_HTTP_PORT`, the listener is disabled. If the port cannot be bound, the error is logged and WebSocket chat keeps running. Every request needs the user's own chat token (see [Requests](#requests)), so the API grants nothing that user's WebSocket `Register` would not.

### Several instances on one server

Each instance has its own `config.py`. Give each one its own `CHAT_HTTP_PORT`. An instance whose port is taken logs `Chat HTTP polling failed to start` and runs without the listener. The same applies to `PBX_NOTIFICATION_PORT` if the PBX fallback is enabled.

```python
# instance 1                        # instance 2
SERVER_PORT = 8765                  SERVER_PORT = 8865
CHAT_HTTP_PORT = 8082               CHAT_HTTP_PORT = 8182
```

The listener binds to `127.0.0.1`, so it is reachable from the same server only:

- **dev_ccss on the same server:** point its `CHAT_HTTP_URL` straight at the instance (`http://127.0.0.1:8082`). nginx is not needed for this.
- **dev_ccss on another server:** add an nginx location per instance next to its WebSocket location, and serve it over HTTPS, because user tokens travel in the `Authorization` header. dev_ccss appends `?token=<NOTIFICATION_WS_QUERY_TOKEN>` unencoded, so the location can check it the way the WebSocket location does. Allowing only the dev_ccss server's IP keeps everyone else off it entirely. The trailing `/` on `proxy_pass` removes the prefix, so the service receives `/chat/...`. Then set `CHAT_HTTP_URL=https://chat.example.com/ws1-http`.

```nginx
# Instance 1: existing WebSocket location, unchanged
location /ws1/ {
    proxy_pass http://127.0.0.1:8765/;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
}

# Instance 1: HTTP chat API
location /ws1-http/ {
    allow 203.0.113.10;     # the dev_ccss server
    deny all;
    proxy_pass http://127.0.0.1:8082/;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    client_max_body_size 1m;
}
```

Repeat both locations for each instance with its own prefix and ports.

## Requests

Every request names the user it acts as with `username` and `organization_id`, and sends that user's chat token (`clients.token`) as `Authorization: Bearer <token>`. These are the credentials the WebSocket `Register` event checks; HTTP has no separate session. A missing token returns `401 missing_token`, and an unknown user or wrong token returns `401 invalid_credentials`. The two cases look the same so usernames cannot be probed. A `token` query parameter (nginx's) is ignored.

- **GET:** parameters go in the query string.
- **POST:** parameters go in a JSON body. Query string parameters are also accepted; the body wins when both are given. An empty body is allowed when there is nothing else to send.

Room endpoints (`/chat/rooms/{room}/…`) require the user to be an active participant of the room in that organization. Otherwise they return `403 not_a_participant`. The WebSocket events do not check this.

## Endpoints

| WebSocket event | HTTP | Parameters | Response |
| --- | --- | --- | --- |
| `Ping` | `GET /chat/ping` | | `{"status": true, "user_id": 7}` |
| `GetUserStatus` | `GET /chat/user-status` | | `{"user_id": 7, "online": true}` (online = has an open WebSocket) |
| `notification` | `POST /chat/notifications` | `title`, `body`, `data` | `{"success": true}`; `username` is the recipient, and the token must be theirs |
| `GetRooms` | `GET /chat/rooms` | | `{"rooms": [...]}` |
| `UpdateOrMakeRoom` | `POST /chat/rooms` | `name`, `users` (array of usernames or IDs), `description`, `type` (`group`/`direct`, optional) | `{"status": "success", "room": 5, "name": "Ops", "type": "group"}`, or `422` with `error` |
| `GetUsersInRoom` | `GET /chat/rooms/{room}/users` | | `{"room": 1, "users": [...], "owners": [...]}` |
| `LeaveRoom` | `POST /chat/rooms/{room}/leave` | | `{"success": true}` |
| `SilentRoom` | `POST /chat/rooms/{room}/silent` | | `{"success": true}` |
| `UnSilentRoom` | `POST /chat/rooms/{room}/unsilent` | | `{"success": true}` |
| `LastSeenMsg` | `POST /chat/rooms/{room}/last-seen` | `msg_id` | `{"success": true}` |
| `ClearLastMessageSeen` | `POST /chat/rooms/{room}/clear-last-seen` | | `{"success": true}` |
| `GetMessagesInRoom` | `GET /chat/rooms/{room}/messages` | `last_id`, `limit` (optional) | see below |
| `GetPrevMessagesInRoom` | `GET /chat/rooms/{room}/messages/previous` | `before_id` | `{"room": 1, "messages": [...]}`, newest first, 20 at most |
| `GetLastMessagesInRoom` | `GET /chat/rooms/{room}/messages/latest` | | `{"room": 1, "messages": [...]}`, newest first, 20 at most |
| `BroadcastMessage` | `POST /chat/rooms/{room}/messages` | `message`, `msginfo` (string or null) | `{"success": true, "msgid": 13}` |
| `EditMessageInRoom` | `POST /chat/rooms/{room}/messages/{msg_id}/edit` | `message`, `msginfo` | `{"success": true, "msgid": 12}` |
| `DeleteMessageInRoom` | `POST /chat/rooms/{room}/messages/{msg_id}/delete` | | `{"success": true, "msgid": 12}` |
| (HTTP only) | `GET /chat/unread` | | unread counts per room, see below |
| `call_notification` | `POST /pbx/notifications` on the PBX listener | | see `pbx-call-notifications.md` |

`success: false` means the same as the WebSocket `*_failed` replies. For example, editing or deleting a message the user did not write returns false, and so does saving an edit whose text did not change.

Over HTTP, sent and edited messages reach other participants as the usual `chat_message` / `chat_message_updated` WebSocket events. As over WebSocket, a sent message is not echoed to the sender's own WebSocket sessions.

### Checking for new messages

`GET /chat/unread`

```json
{
  "user_id": 7,
  "has_new_messages": true,
  "total_unread": 2,
  "rooms": [
    {"room": 1, "name": "Ops", "room_type": "group", "last_message_at": "2026-10-19T09:00:00",
     "last_message_seen": 10, "last_message_id": 12, "unread_count": 2}
  ]
}
```

`unread_count` counts non-deleted messages with `id > last_message_seen`, not counting the user's own messages. Only rooms the user has not left are listed. Reading messages does not mark them as read; call `POST /chat/rooms/{room}/last-seen` for that.

`GET /chat/rooms/{room}/messages?last_id=10&limit=20`

```json
{
  "room": 1,
  "last_id": 10,
  "next_last_id": 12,
  "has_more": false,
  "messages": [
    {"id": 11, "user_id": 3, "username": "ann", "room_id": 1, "message": "hi",
     "message_information": null, "created_at": "2026-10-19T09:00:00", "updated_at": "2026-10-19T09:00:00"}
  ]
}
```

Messages are oldest first. If `last_id` is omitted, the user's `last_message_seen` is used. `limit` defaults to 20, with a maximum of 100. Pass `next_last_id` as `last_id` on the next poll, and poll again straight away when `has_more` is true. `GET /chat/messages?room=1` is the same endpoint.

## Errors

| Status | `error` |
| --- | --- |
| 400 | `invalid_json`, `expected_json_object`, `invalid_organization_id`, `invalid_username`, `invalid_room`, `invalid_last_id`, `invalid_before_id`, `invalid_limit`, `invalid_msg_id`, `invalid_message`, `invalid_msginfo`, `invalid_name`, `invalid_users`, `invalid_description`, `invalid_type` |
| 401 | `missing_token`, `invalid_credentials` |
| 403 | `not_a_participant` |
| 422 | room creation/update refused (`error` holds the reason) |
| 500 | `request_failed` (details in the service log) |

## Laravel example

```php
$client = ConversationClients::forUser($organizationId, $username)->first();
$chat = Http::withToken($client->token)
    ->baseUrl(config('services.chat_http.url'))
    ->timeout(10);
$who = ['username' => $client->username, 'organization_id' => $organizationId];

$unread = $chat->get('/chat/unread', $who)->json();

$sent = $chat->post("/chat/rooms/{$roomId}/messages", $who + [
    'message' => $text,
    'msginfo' => null,
])->json();

$chat->post("/chat/rooms/{$roomId}/last-seen", $who + ['msg_id' => $sent['msgid']]);
```

dev_ccss's `ChatHttpClient` does this for the signed-in user and also appends the nginx query token.

Sending a message waits for Firebase pushes to offline recipients before it responds, as over WebSocket, so allow a timeout of several seconds.
