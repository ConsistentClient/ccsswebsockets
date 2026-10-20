"""iOS VoIP (PushKit) pushes for incoming calls, sent straight to APNs: Firebase cannot send them.

Signed with the Apple push key placed next to this file as AuthKey_<KEY_ID>.p8, the name Apple
downloads it under. The team ID, bundle ID and VoIP token come from the app with its device token,
so nothing is configured: with no key file, calls to iPhones fall back to the ordinary alert.
Needs the h2 package (pip install "httpx[http2]").
"""

import json
import threading
import time
from pathlib import Path

import httpx
import jwt

HOSTS = {
    "production": "https://api.push.apple.com",
    "development": "https://api.sandbox.push.apple.com",
}
# Apple refuses provider tokens older than an hour and throttles ones renewed more often than every 20 minutes.
TOKEN_LIFETIME = 40 * 60
# A device token rejected for either of these will not work again.
GONE_REASONS = {"Unregistered", "BadDeviceToken", "DeviceTokenNotForTopic"}


class ApnsVoip:
    def __init__(self, directory, logger, client_factory=None):
        self.directory = Path(directory)
        self.logger = logger
        self.client_factory = client_factory or (lambda: httpx.Client(http2=True, timeout=10))
        self._lock = threading.Lock()
        self._client = None
        self._key = None
        self._provider_tokens = {}
        # Which APNs environment accepted each VoIP token: development builds use the sandbox.
        self._environments = {}

    def _signing_key(self):
        if self._key is None:
            keys = sorted(self.directory.glob("AuthKey_*.p8"))
            if keys:
                self._key = (keys[0].stem[len("AuthKey_"):], keys[0].read_text())
        return self._key

    def _provider_token(self, team_id, key_id, private_key):
        cached = self._provider_tokens.get(team_id)
        now = int(time.time())
        if cached and now - cached[1] < TOKEN_LIFETIME:
            return cached[0]
        token = jwt.encode({"iss": team_id, "iat": now}, private_key, algorithm="ES256", headers={"kid": key_id})
        self._provider_tokens[team_id] = (token, now)
        return token

    def send(self, voip_token, bundle_id, team_id, payload):
        """Returns "ok", "unregistered" (the token will never work), "error", or None when VoIP is not set up."""
        key = self._signing_key()
        if key is None or not voip_token or not bundle_id or not team_id:
            return None
        body = json.dumps(dict(payload, aps={})).encode()
        known = self._environments.get(voip_token)
        environments = [known] if known else ["production", "development"]
        with self._lock:
            try:
                if self._client is None:
                    self._client = self.client_factory()
                for index, environment in enumerate(environments):
                    response = self._client.post(
                        HOSTS[environment] + "/3/device/" + voip_token,
                        content=body,
                        headers={
                            "authorization": "bearer " + self._provider_token(team_id, *key),
                            "apns-topic": bundle_id + ".voip",
                            "apns-push-type": "voip",
                            "apns-priority": "10",
                            "apns-expiration": str(int(time.time()) + 30),
                        },
                    )
                    if response.status_code == 200:
                        self._environments[voip_token] = environment
                        return "ok"
                    try:
                        reason = response.json().get("reason")
                    except ValueError:
                        reason = None
                    # A sandbox token is a BadDeviceToken in production; try the other environment first.
                    if reason == "BadDeviceToken" and index + 1 < len(environments):
                        continue
                    self.logger.warning("APNs VoIP push refused: status=%s reason=%s environment=%s",
                                        response.status_code, reason, environment)
                    if reason in ("ExpiredProviderToken", "InvalidProviderToken"):
                        self._provider_tokens.pop(team_id, None)
                    if response.status_code == 410 or reason in GONE_REASONS:
                        self._environments.pop(voip_token, None)
                        return "unregistered"
                    return "error"
            except ImportError:
                self.logger.error("APNs VoIP push unavailable: install the h2 package (pip install \"httpx[http2]\")")
                return None
            except Exception as error:
                self.logger.exception("APNs VoIP push failed: error_type=%s", type(error).__name__)
                # A broken connection is replaced on the next push.
                self._client = None
                return "error"
        return "error"
