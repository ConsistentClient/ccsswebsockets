# PBX call notifications

`dev_ccss` sends each eligible employee's extension, recipient username and organization ID, WebSocket URL, and shared `NOTIFICATION_WS_USERNAME` / `NOTIFICATION_WS_PASSWORD` to `ccsspbx`. Each recipient mapping carries the shared notification account credentials. Individual chat credentials, SIP passwords, Firebase credentials, and device tokens are never sent.

The PBX authenticates the shared account using `Register`, then sends `event=call_notification` with the separate recipient username/organization. `ccsswebsockets` reads the recipient's device tokens from `clients` and sends Firebase `data.type=incoming_call`. Only organization 0 senders (the shared notification account) can send call events, and they can target any tenant; other accounts are rejected with `recipient_not_authorized`. Every call event requires its registration session token.

Calls reach iPhones as VoIP (PushKit) pushes shown in CallKit and Android phones as full-screen calls, when the installed app supports it; see [How calls reach each phone](#how-calls-reach-each-phone).

## Deploy and initialize

Deploy `main.py` and `pbx_notifications.py`, then restart `ccsswebsockets`. Synchronized mappings use the existing WebSocket endpoint; a new HTTP listener is unnecessary.

In `ccsspbx`, run the extension migrations and enable the event bridge:

```dotenv
FREESWITCH_BRIDGE_EVENTS_ENABLED=true
```

```bash
php artisan migrate
php artisan config:cache
php artisan queue:restart
```

Keep `php artisan freeswitch:bridge-events` and an asynchronous queue worker running. Synchronous queues block event processing during a push. Jobs store extension/call metadata, load current credentials during processing, discard alerts older than 30 seconds, and do not retry delivery automatically.

Call notification settings belong to each PBX extension: `call_notifications_enabled`, `notification_url`, and encrypted `notification_secret`. Configure them in the extension editor. The URL/secret are used for manual HTTP user-ID mappings; synchronized username mappings use the extension's saved WebSocket connection credentials. The old `CCSSWEBSOCKETS_*` environment settings are no longer read. Existing extensions start disabled after the new settings migration; enable the desired extensions in their editor.

In `dev_ccss`, configure the same shared notification account already used for notifications:

```dotenv
NOTIFICATION_WS_USERNAME=YOUR_NOTIFICATION_ACCOUNT
NOTIFICATION_WS_PASSWORD=YOUR_NOTIFICATION_PASSWORD
NOTIFICATION_WS_URL=wss://YOUR_CHAT_HOST/YOUR_WEBSOCKET_PATH/
```

`NOTIFICATION_WS_TOKEN` is a fallback when the password variable is absent. With no explicit URL, `DB_CHAT_CONNECTION_DOMAIN` plus `DB_CHAT_CONNECTION_WS` supplies it. An optional `NOTIFICATION_WS_QUERY_TOKEN` is appended to the URL.

The organization's existing PBX settings need phone service enabled, an enabled SIP server/domain, API base URL (or SIP server from which the HTTPS base URL can be inferred), and its existing PBX API token. That token must belong to an administrator or an API Status Reader scoped to the PBX organization and pass its IP allowlist. PBX organizations are resolved by domain; the external recipient organization ID comes from dev_ccss and may differ from the PBX ID.

```bash
php artisan config:cache
php artisan queue:restart
php artisan pbx:sync-notification-recipients ORGANIZATION_ID
```

Run these in dev_ccss. Without an organization argument, the sync command queues all configured organizations. Keep its existing scheduler and asynchronous queue worker running. Run the required `php artisan ai:validate-semantic-layer --check-schema` against the configured database at deployment; dictionary validation passes locally, but local schema validation could not connect to a database.

## Sync frequency and removal

Relevant employee create/edit/delete/restore operations, extension edits, chat account changes, and PBX settings changes queue an organization snapshot after transaction commit. Closely spaced edits coalesce into a pending job with a two-second delay. Jobs read current data and retry failed snapshots up to three attempts. Existing five-minute PBX reconciliation also queues snapshots, including disabled phone services so stale mappings clear. Environment credential changes need config refresh and explicit sync or the next reconciliation.

Snapshots include active employees with remote access, a username, and a phone extension. Explicit `extension@domain` values must match the PBX domain. Recipient mappings follow the app's first enabled PBX configuration; other configured destinations receive empty snapshots. Duplicate employee extensions fail the snapshot rather than choose a recipient.

The PBX endpoint is `POST /api/dev-ccss/call-notification-recipients` using the existing Sanctum Bearer API token. Unknown extensions reject the entire snapshot. Removed recipients clear only sync-managed mappings, preserving manual mappings. WebSocket URL/password are encrypted with PBX `APP_KEY`; URL, sender username, and password are hidden from extension serialization. Reconciliation skips unchanged encrypted writes.

Sync enables new or reactivated recipient mappings and preserves a local disable choice on an existing mapped extension. The optional per-recipient `call_notifications_enabled` boolean explicitly overrides that choice when provided. Removing a managed recipient disables its notifications. Both event detection and queued delivery check the current per-extension enable flag.

Before entirely removing or replacing a PBX configuration/domain/API URL, clear the old destination while its credentials remain configured. Snapshots target currently configured destinations.

## WebSocket contract

```json
{"event":"Register","username":"NOTIFICATION_ACCOUNT","token":"NOTIFICATION_PASSWORD"}
```

Use the returned session token in the call request:

```json
{"event":"call_notification","username":"George","organization_id":1,"data":{"session_token":"REGISTRATION_SESSION_TOKEN","call_id":"UNIQUE_CALL_UUID","caller_name":"Test caller","caller_number":"100","extension":"200"}}
```

Success returns `event=call_notification_success`, `status=accepted`, and `accepted>0`. Firebase acceptance does not confirm phone display. Failures return `call_notification_failed` with an error or device counts. Call events are processed concurrently, so replies can arrive out of order; every reply carries the request's `call_id`. `organization_id` may be a number or a numeric string, and null caller fields are treated as empty. Logs include call/recipient identifiers and counts, without passwords or full device tokens.

Call pushes are sent even when recipients have active WebSockets. Android uses high priority with a 30-second TTL; APNs pushes (alert or VoIP) expire after 30 seconds.

### How calls reach each phone

Each device entry in `clients.device_token` carries what the app said about the device when it registered its token (`POST /api/storeToken` in dev_ccss): `platform`, `call_push`, and on iPhone `voip_token`, `bundle_id` and `team_id`. Every registration replaces them, so a device always gets what its installed app version handles:

| Device | Call push |
| --- | --- |
| iPhone with a `voip_token` | VoIP (PushKit) push sent straight to APNs: wakes the app, which shows the call in CallKit. If it cannot be sent, the ordinary alert below is sent instead. |
| Android with `call_push` | Data-only, high-priority FCM message: the app shows a full-screen incoming call, also from the background. |
| Anything else (older app versions) | The ordinary alert, as before. |

The call data (all strings) is `type=incoming_call`, `action`, `call_id`, `callkit_id` (one UUID per call, so a repeated push shows as one call), `caller`, `caller_name`, `caller_number`, `extension`, `title` and `body`. Chat and other notifications are unchanged.

VoIP pushes are signed with the Apple push key: download it in the Apple Developer account (Keys, with Apple Push Notifications service enabled) and put it, under the name Apple gives it (`AuthKey_<KEY_ID>.p8`), next to `main.py`, then restart. The key ID comes from the file name and the team and bundle IDs from the app, so nothing else is configured; production and sandbox (development build) tokens are both handled. APNs needs HTTP/2: `pip install "httpx[http2]"` in the service's environment. Without the key file or the package, iPhones get the ordinary alert and the reason is logged. A VoIP token APNs rejects as no longer valid is removed from the device entry, which then gets ordinary alerts until the app registers a new one.

The PBX holds the ring of an unregistered app extension for up to 20 seconds while the woken app registers (see fusionapi's ring hook). A push cannot start an Android app the user force-stopped.

## Organization isolation

Each WebSocket session takes its organization from the account it registered with. Accounts outside organization 0 are confined to their own organization:

- `call_notification`: rejected for every sender outside organization 0.
- `notification`: the target `organization_id` must equal the sender's, and the recipient username is resolved within that organization. Organization 0 senders can target any tenant.
- Chat rooms (`UpdateOrMakeRoom`): participants are resolved within the creator's organization, whether given as usernames or as numeric user IDs. A participant that does not belong to that organization is dropped from the room.
- Chat messages (`BroadcastMessage`): the sender must be a participant of the room, and delivery over WebSocket skips any participant whose session belongs to another organization. Push notifications for offline participants look up device tokens within the sender's organization.

Rooms created before numeric IDs were checked may still hold participant rows from another organization. Those participants no longer receive messages, but the rows are not removed automatically.

## Test

After syncing, run in ccsspbx:

```bash
php artisan call-push:test EXTENSION_DATABASE_ID --caller="CCSS test caller"
```

Use the extension database ID, not its number. The command sends immediately without a live call. Event triggers support `CHANNEL_EXECUTE` / `bridge` with `user/EXT@DOMAIN`, and outbound `CHANNEL_ORIGINATE` with `dialed_user`/`dialed_domain` or `presence_id`. Other dial string forms need additional mapping.

Watch `PBX call notification`, `FCM send attempt`, and `PBX push result` in the Python error log, and recipient-sync/call-result messages in Laravel logs.

The earlier manual chat user-ID/org-ID mapping remains supported via optional HTTP `/pbx/notifications`. That fallback requires Python `PBX_NOTIFICATION_SECRET` (host/port default to 127.0.0.1:8081; if the listener cannot bind, the error is logged and the service continues without it), and the PBX extension's `notification_url` / `notification_secret`. Set the extension secret to the Python endpoint's shared secret. Synchronized username mappings use WebSocket credentials instead.
