"""Claude subscription (OAuth) upstream support.

This implements a first-class "oauth" upstream auth mode for Claudius. Instead of
proxying requests through the controller with an upstream API key, the controller
holds an OAuth credential for a Claude Pro/Max account (the same credential
``claude setup-token`` produces) and injects it into worker sessions via the
``CLAUDE_CODE_OAUTH_TOKEN`` environment variable. Claude Code then talks directly
to ``api.anthropic.com`` with the OAuth bearer.

NOTE (tech debt): OAuth sessions bypass the Claudius proxy entirely, so per-request
cost/token accounting and upstream rewriting do not apply to them. Routing OAuth
through the proxy (controller attaches the bearer + the OAuth beta header) is a
future improvement.

The public Claude Code OAuth client uses a fixed redirect URI that *displays* the
authorization code for copy-paste, so the login flow is: build an authorize URL ->
user authorizes and copies the ``code#state`` value -> controller exchanges it for
tokens. See ``OAuthManager``.
"""

from __future__ import annotations

import abc
import base64
import hashlib
import logging
import secrets
import time
from dataclasses import dataclass
from urllib.parse import urlencode

import httpx

logger = logging.getLogger(__name__)

# Public Claude Code OAuth client. These match what ``claude setup-token`` uses so
# the credential we obtain is interchangeable with one minted by the CLI.
DEFAULT_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
DEFAULT_AUTHORIZE_URL = "https://claude.ai/oauth/authorize"
DEFAULT_TOKEN_URL = "https://console.anthropic.com/v1/oauth/token"
DEFAULT_REDIRECT_URI = "https://console.anthropic.com/oauth/code/callback"
DEFAULT_SCOPES = "org:create_api_key user:profile user:inference"

# Refresh a little before the token actually expires so an in-flight session start
# never gets handed a token that dies mid-request.
_REFRESH_LEEWAY_SECONDS = 120.0


@dataclass
class OAuthCredentials:
    access_token: str
    refresh_token: str | None
    expires_at: float  # epoch seconds; 0 means "unknown / never expires"
    scope: str = ""
    token_type: str = "Bearer"

    def is_expired(self, *, leeway: float = 0.0) -> bool:
        if not self.expires_at:
            return False
        return time.time() >= (self.expires_at - leeway)

    def to_row(self) -> dict[str, object]:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at": self.expires_at,
            "scope": self.scope,
            "token_type": self.token_type,
        }

    @classmethod
    def from_row(cls, row: dict[str, object]) -> "OAuthCredentials":
        return cls(
            access_token=str(row["access_token"]),
            refresh_token=(str(row["refresh_token"]) if row.get("refresh_token") else None),
            expires_at=float(row.get("expires_at") or 0.0),
            scope=str(row.get("scope") or ""),
            token_type=str(row.get("token_type") or "Bearer"),
        )


class OAuthTokenStore(abc.ABC):
    """Persistence boundary for the single shared OAuth credential.

    Implementations only need to durably round-trip one credential. The
    ``OAuthManager`` keeps the authoritative copy in memory, so reads on the hot
    path (session env building) never touch the store.
    """

    @abc.abstractmethod
    async def load(self) -> OAuthCredentials | None: ...

    @abc.abstractmethod
    async def save(self, creds: OAuthCredentials) -> None: ...

    @abc.abstractmethod
    async def clear(self) -> None: ...


class InMemoryOAuthTokenStore(OAuthTokenStore):
    """Ephemeral store. The credential is lost on controller restart."""

    def __init__(self) -> None:
        self._creds: OAuthCredentials | None = None

    async def load(self) -> OAuthCredentials | None:
        return self._creds

    async def save(self, creds: OAuthCredentials) -> None:
        self._creds = creds

    async def clear(self) -> None:
        self._creds = None


class SqliteOAuthTokenStore(OAuthTokenStore):
    """Durable store backed by the controller database.

    Survives restarts only if the database file itself is on a persistent volume.
    """

    _PROVIDER = "anthropic"

    def __init__(self, db) -> None:
        self._db = db

    async def load(self) -> OAuthCredentials | None:
        row = await self._db.get_oauth_token(self._PROVIDER)
        return OAuthCredentials.from_row(row) if row else None

    async def save(self, creds: OAuthCredentials) -> None:
        await self._db.set_oauth_token(self._PROVIDER, creds.to_row())

    async def clear(self) -> None:
        await self._db.clear_oauth_token(self._PROVIDER)


@dataclass
class OAuthClientConfig:
    client_id: str = DEFAULT_CLIENT_ID
    authorize_url: str = DEFAULT_AUTHORIZE_URL
    token_url: str = DEFAULT_TOKEN_URL
    redirect_uri: str = DEFAULT_REDIRECT_URI
    scopes: str = DEFAULT_SCOPES


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def generate_pkce() -> tuple[str, str]:
    """Return (verifier, challenge) for a PKCE S256 exchange."""
    verifier = _b64url(secrets.token_bytes(32))
    challenge = _b64url(hashlib.sha256(verifier.encode("ascii")).digest())
    return verifier, challenge


class OAuthError(Exception):
    pass


class OAuthManager:
    """Owns the shared Claude OAuth credential and the login flow.

    The authoritative credential is cached in memory so ``access_token()`` /
    ``is_connected()`` are synchronous and cheap (called while building session
    env). ``ensure_fresh()`` refreshes the cached + persisted credential when it is
    close to expiry and must be awaited before a session reads the token.
    """

    def __init__(
        self,
        store: OAuthTokenStore,
        config: OAuthClientConfig | None = None,
    ) -> None:
        self._store = store
        self._config = config or OAuthClientConfig()
        self._creds: OAuthCredentials | None = None
        # Pending PKCE logins keyed by state. Ephemeral by design.
        self._pending: dict[str, str] = {}

    async def load(self) -> None:
        """Load any persisted credential into the in-memory cache at startup."""
        try:
            self._creds = await self._store.load()
        except Exception:  # pragma: no cover - defensive; never block startup
            logger.exception("failed to load persisted OAuth credential")
            self._creds = None
        if self._creds:
            logger.info("loaded persisted Claude OAuth credential")

    # -- synchronous reads (hot path) ------------------------------------

    def is_connected(self) -> bool:
        return self._creds is not None

    def access_token(self) -> str | None:
        return self._creds.access_token if self._creds else None

    def status(self) -> dict[str, object]:
        if not self._creds:
            return {"connected": False}
        return {
            "connected": True,
            "expires_at": self._creds.expires_at or None,
            "expired": self._creds.is_expired(),
            "scope": self._creds.scope,
            "can_refresh": bool(self._creds.refresh_token),
        }

    # -- login flow ------------------------------------------------------

    def start_login(self) -> dict[str, str]:
        """Begin a PKCE login. Returns the authorize URL the user must visit."""
        verifier, challenge = generate_pkce()
        state = _b64url(secrets.token_bytes(24))
        self._pending[state] = verifier
        # Cap pending logins so an abandoned-login spammer can't grow this forever.
        if len(self._pending) > 16:
            for stale in list(self._pending)[:-16]:
                self._pending.pop(stale, None)
        params = {
            "code": "true",
            "client_id": self._config.client_id,
            "response_type": "code",
            "redirect_uri": self._config.redirect_uri,
            "scope": self._config.scopes,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "state": state,
        }
        authorize_url = f"{self._config.authorize_url}?{urlencode(params)}"
        return {"authorize_url": authorize_url, "state": state}

    async def complete_login(self, pasted: str) -> dict[str, object]:
        """Exchange the pasted ``code#state`` value for tokens and store them."""
        pasted = pasted.strip()
        if not pasted:
            raise OAuthError("authorization code is empty")
        if "#" in pasted:
            code, _, state = pasted.partition("#")
        else:
            code, state = pasted, ""
        code = code.strip()
        state = state.strip()
        verifier = self._pending.pop(state, None) if state else None
        if verifier is None:
            # Fall back to the most recent pending login if the pasted value didn't
            # carry a recognizable state (some users paste only the code).
            if len(self._pending) == 1:
                state, verifier = next(iter(self._pending.items()))
                self._pending.pop(state, None)
            else:
                raise OAuthError("no matching pending login; start the login again")

        payload = {
            "grant_type": "authorization_code",
            "code": code,
            "state": state,
            "client_id": self._config.client_id,
            "redirect_uri": self._config.redirect_uri,
            "code_verifier": verifier,
        }
        creds = await self._token_request(payload)
        await self._set(creds)
        logger.info("Claude OAuth login completed (scope=%s)", creds.scope or "?")
        return self.status()

    async def ensure_fresh(self) -> OAuthCredentials | None:
        """Refresh the cached credential if it is near expiry. Returns the current one."""
        creds = self._creds
        if creds is None:
            return None
        if not creds.is_expired(leeway=_REFRESH_LEEWAY_SECONDS):
            return creds
        if not creds.refresh_token:
            logger.warning("Claude OAuth token expired and no refresh token is available")
            return creds
        try:
            refreshed = await self._token_request({
                "grant_type": "refresh_token",
                "refresh_token": creds.refresh_token,
                "client_id": self._config.client_id,
            })
        except OAuthError:
            logger.exception("failed to refresh Claude OAuth token")
            return creds
        # Anthropic may omit a new refresh token; keep the existing one if so.
        if not refreshed.refresh_token:
            refreshed.refresh_token = creds.refresh_token
        await self._set(refreshed)
        logger.info("refreshed Claude OAuth token")
        return refreshed

    async def logout(self) -> None:
        self._creds = None
        self._pending.clear()
        await self._store.clear()
        logger.info("cleared Claude OAuth credential")

    # -- internals -------------------------------------------------------

    async def _set(self, creds: OAuthCredentials) -> None:
        self._creds = creds
        try:
            await self._store.save(creds)
        except Exception:  # pragma: no cover - persistence is best-effort
            logger.exception("failed to persist OAuth credential (continuing in-memory)")

    async def _token_request(self, payload: dict[str, object]) -> OAuthCredentials:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    self._config.token_url,
                    json=payload,
                    headers={"Content-Type": "application/json", "Accept": "application/json"},
                )
        except httpx.HTTPError as exc:
            raise OAuthError(f"token endpoint request failed: {exc}") from exc
        if resp.status_code >= 400:
            raise OAuthError(f"token endpoint returned {resp.status_code}: {resp.text[:500]}")
        try:
            data = resp.json()
        except ValueError as exc:
            raise OAuthError("token endpoint returned a non-JSON response") from exc
        access_token = data.get("access_token")
        if not access_token:
            raise OAuthError("token endpoint response missing access_token")
        expires_in = data.get("expires_in")
        expires_at = (time.time() + float(expires_in)) if expires_in else 0.0
        return OAuthCredentials(
            access_token=str(access_token),
            refresh_token=(str(data["refresh_token"]) if data.get("refresh_token") else None),
            expires_at=expires_at,
            scope=str(data.get("scope") or ""),
            token_type=str(data.get("token_type") or "Bearer"),
        )
