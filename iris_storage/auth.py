"""Service identity sent to iris-storage on every call.

iris-storage verifies who is calling (shadow first, then enforced). The mode is
chosen by IRIS_STORAGE_AUTH:

- ``google``: a Google ID token for IRIS_STORAGE_AUDIENCE (the service's URL),
  fetched from the runtime's credentials (Cloud Run, GKE Workload Identity) and
  refreshed before it expires.
- ``static``: the token in IRIS_STORAGE_TOKEN, minted by the local stack
  (local.sh) for this service.
- ``none``: no header.

Unset, the mode is ``static`` when IRIS_STORAGE_TOKEN is set, ``google`` when
IRIS_STORAGE_AUDIENCE is set, else ``none``: a service configured for neither
keeps calling exactly as before.
"""

import logging
import os
import threading
import time
from typing import Optional

import httpx

logger = logging.getLogger("iris.storage")

# Google ID tokens live one hour; refresh well before that.
_GOOGLE_TOKEN_TTL = 50 * 60


def auth_mode() -> str:
    mode = os.getenv("IRIS_STORAGE_AUTH", "").strip().lower()
    if mode:
        return mode
    if os.getenv("IRIS_STORAGE_TOKEN"):
        return "static"
    if os.getenv("IRIS_STORAGE_AUDIENCE"):
        return "google"
    return "none"


class ServiceIdentityAuth(httpx.Auth):
    """httpx auth adding ``Authorization: Bearer <service token>``.

    One instance is shared by the sync and async clients and by worker threads;
    a token fetch failure is logged and the request goes out without a header
    (iris-storage decides what that means: logged in shadow, refused when
    enforced).
    """

    def __init__(self, mode: Optional[str] = None):
        self.mode = mode or auth_mode()
        self._lock = threading.Lock()
        self._token: Optional[str] = None
        self._expires_at = 0.0
        if self.mode not in ("google", "static", "none"):
            logger.error("IRIS_STORAGE_AUTH=%r is not google, static or none: no identity sent", self.mode)
            self.mode = "none"
        elif self.mode == "static" and not os.getenv("IRIS_STORAGE_TOKEN"):
            logger.error("IRIS_STORAGE_AUTH=static but IRIS_STORAGE_TOKEN is empty: no identity sent")
        elif self.mode == "google" and not os.getenv("IRIS_STORAGE_AUDIENCE"):
            logger.error("IRIS_STORAGE_AUTH=google but IRIS_STORAGE_AUDIENCE is empty: no identity sent")

    def token(self) -> Optional[str]:
        if self.mode == "static":
            return os.getenv("IRIS_STORAGE_TOKEN") or None
        if self.mode != "google":
            return None
        with self._lock:
            if self._token and time.monotonic() < self._expires_at:
                return self._token
            audience = os.getenv("IRIS_STORAGE_AUDIENCE")
            if not audience:
                return None
            try:
                import google.auth.transport.requests
                import google.oauth2.id_token

                request = google.auth.transport.requests.Request()
                self._token = google.oauth2.id_token.fetch_id_token(request, audience)
                self._expires_at = time.monotonic() + _GOOGLE_TOKEN_TTL
            except Exception as exc:  # no credentials, metadata server down...
                logger.warning("iris-storage ID token fetch failed (%s): calling without identity", exc)
                self._token = None
                self._expires_at = time.monotonic() + 30  # retry soon, not on every call
            return self._token

    def auth_flow(self, request: httpx.Request):
        token = self.token()
        if token:
            request.headers["Authorization"] = f"Bearer {token}"
        yield request
