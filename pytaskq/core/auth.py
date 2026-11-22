"""
pytaskq.core.auth — users, roles, sessions, invites and API tokens
===================================================================

Everything lives in Redis next to the jobs, so a self-hosted install needs no
extra database and survives restarts as long as Redis persists (Docker Compose
already mounts a volume for it).

Security model
--------------
* Passwords are hashed with scrypt (stdlib, memory-hard, per-user random salt).
* Session cookies, invite links and API tokens are random 256-bit secrets.
  Redis stores only their SHA-256, so a Redis dump does not hand out logins.
* Roles are ordered: viewer < operator < admin.
* The first admin is created with a one-time setup code printed in the server
  log (or headlessly from PYTASKQ_ADMIN_USER / PYTASKQ_ADMIN_PASSWORD).
* Repeated failed logins are throttled per username and per IP.

Redis keys
----------
auth:users                    SET   usernames
auth:user:{username}          HASH  username, role, password_hash, created_at, …
auth:session:{sha}            STR   username                (TTL = session lifetime)
auth:user_sessions:{username} SET   session hashes          (to revoke all on reset)
auth:invites                  SET   invite hashes
auth:invite:{sha}             HASH  id, role, created_by, … (TTL = 7 days)
auth:token:{sha}              HASH  id, username, name, created_at, last_used_at
auth:user_tokens:{username}   SET   token hashes
auth:setup_code               STR   one-time code for creating the first admin
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import re
import secrets
import time
from dataclasses import asdict, dataclass
from typing import Optional

from pytaskq.core.config import settings

ROLES = ("viewer", "operator", "admin")
ROLE_LEVEL = {role: i for i, role in enumerate(ROLES, start=1)}

USERNAME_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{1,31}$")
MIN_PASSWORD = 8
MAX_PASSWORD = 256

INVITE_TTL = 7 * 24 * 3600
LOGIN_WINDOW = 15 * 60
MAX_FAILS_PER_USER = 10
MAX_FAILS_PER_IP = 30

TOKEN_PREFIX = "ptq_"

K_USERS = "auth:users"
K_INVITES = "auth:invites"
K_SETUP_CODE = "auth:setup_code"


class AuthError(Exception):
    """A user-facing auth problem. `status` maps to the HTTP status code."""

    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.message = message
        self.status = status


@dataclass
class User:
    username: str
    role: str
    created_at: float = 0.0
    created_by: str = ""
    last_login_at: float = 0.0
    disabled: bool = False
    via: str = "session"      # session | token | master_key | auth_off

    def can(self, role: str) -> bool:
        return not self.disabled and ROLE_LEVEL.get(self.role, 0) >= ROLE_LEVEL[role]

    def public(self) -> dict:
        d = asdict(self)
        d.pop("via", None)
        return d


def _sha(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def _k_user(username: str) -> str:
    return f"auth:user:{username}"


# ── password hashing ─────────────────────────────────────────────────────────

_SCRYPT = dict(n=2 ** 14, r=8, p=1, dklen=64)


def _hash_password_sync(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, maxmem=64 * 1024 * 1024, **_SCRYPT)
    b64 = lambda b: base64.b64encode(b).decode()  # noqa: E731
    return f"scrypt${_SCRYPT['n']}${_SCRYPT['r']}${_SCRYPT['p']}${b64(salt)}${b64(digest)}"


def _verify_password_sync(password: str, encoded: str) -> bool:
    try:
        algo, n, r, p, salt_b64, digest_b64 = encoded.split("$")
        if algo != "scrypt":
            return False
        expected = base64.b64decode(digest_b64)
        digest = hashlib.scrypt(
            password.encode(), salt=base64.b64decode(salt_b64),
            n=int(n), r=int(r), p=int(p), dklen=len(expected), maxmem=64 * 1024 * 1024,
        )
        return hmac.compare_digest(digest, expected)
    except (ValueError, TypeError):
        return False


async def hash_password(password: str) -> str:
    # scrypt is deliberately slow (~50 ms); keep it off the event loop
    return await asyncio.to_thread(_hash_password_sync, password)


async def verify_password(password: str, encoded: str) -> bool:
    return await asyncio.to_thread(_verify_password_sync, password, encoded)


# Used to spend the same time on unknown usernames as on wrong passwords,
# so response timing doesn't reveal which usernames exist.
_DUMMY_HASH = _hash_password_sync(secrets.token_hex(8))


# ── validation ───────────────────────────────────────────────────────────────

def normalize_username(username: str) -> str:
    name = (username or "").strip().lower()
    if not USERNAME_RE.match(name):
        raise AuthError(
            "Usernames are 2–32 characters: lowercase letters, numbers, dots, dashes or underscores, "
            "starting with a letter or number."
        )
    return name


def check_password_rules(password: str) -> None:
    if not isinstance(password, str) or len(password) < MIN_PASSWORD:
        raise AuthError(f"Passwords need at least {MIN_PASSWORD} characters.")
    if len(password) > MAX_PASSWORD:
        raise AuthError(f"Passwords can be at most {MAX_PASSWORD} characters.")


def check_role(role: str) -> str:
    if role not in ROLES:
        raise AuthError(f"Role must be one of: {', '.join(ROLES)}.")
    return role


# ── users ────────────────────────────────────────────────────────────────────

def _user_from_hash(h: dict) -> User:
    return User(
        username=h["username"],
        role=h.get("role", "viewer"),
        created_at=float(h.get("created_at") or 0),
        created_by=h.get("created_by", ""),
        last_login_at=float(h.get("last_login_at") or 0),
        disabled=h.get("disabled") == "1",
    )


async def user_count(r) -> int:
    return await r.scard(K_USERS)


async def get_user(r, username: str) -> Optional[User]:
    h = await r.hgetall(_k_user(username))
    return _user_from_hash(h) if h else None


async def list_users(r) -> list[User]:
    names = sorted(await r.smembers(K_USERS))
    users = []
    for name in names:
        u = await get_user(r, name)
        if u:
            users.append(u)
    return users


async def create_user(r, username: str, password: str, role: str, created_by: str = "") -> User:
    username = normalize_username(username)
    check_password_rules(password)
    check_role(role)
    pw_hash = await hash_password(password)
    now = time.time()
    # HSETNX on the username field makes "create" atomic: two people accepting
    # invites with the same name can't overwrite each other.
    if not await r.hsetnx(_k_user(username), "username", username):
        raise AuthError(f"The username '{username}' is taken.", 409)
    async with r.pipeline(transaction=True) as pipe:
        pipe.hset(_k_user(username), mapping={
            "role": role, "password_hash": pw_hash, "created_at": now,
            "created_by": created_by, "disabled": "0",
        })
        pipe.sadd(K_USERS, username)
        await pipe.execute()
    return User(username=username, role=role, created_at=now, created_by=created_by)


async def _active_admins(r) -> list[str]:
    return [u.username for u in await list_users(r) if u.role == "admin" and not u.disabled]


async def update_user(r, username: str, *, role: Optional[str] = None,
                      disabled: Optional[bool] = None, acting: Optional[User] = None) -> User:
    user = await get_user(r, username)
    if not user:
        raise AuthError("User not found.", 404)
    losing_admin = user.role == "admin" and not user.disabled and (
        (role is not None and role != "admin") or disabled is True
    )
    if losing_admin and await _active_admins(r) == [username]:
        raise AuthError("This is the only active admin. Make someone else an admin first.", 409)
    changes = {}
    if role is not None:
        changes["role"] = check_role(role)
    if disabled is not None:
        changes["disabled"] = "1" if disabled else "0"
    if changes:
        await r.hset(_k_user(username), mapping=changes)
    if disabled:
        await revoke_all_sessions(r, username)
    return await get_user(r, username)


async def delete_user(r, username: str) -> None:
    user = await get_user(r, username)
    if not user:
        raise AuthError("User not found.", 404)
    if user.role == "admin" and not user.disabled and await _active_admins(r) == [username]:
        raise AuthError("This is the only active admin. Make someone else an admin first.", 409)
    await revoke_all_sessions(r, username)
    for token_sha in await r.smembers(f"auth:user_tokens:{username}"):
        await r.delete(f"auth:token:{token_sha}")
    async with r.pipeline(transaction=True) as pipe:
        pipe.delete(_k_user(username), f"auth:user_tokens:{username}")
        pipe.srem(K_USERS, username)
        await pipe.execute()


async def set_password(r, username: str, new_password: str, keep_session_sha: Optional[str] = None) -> None:
    check_password_rules(new_password)
    if not await r.exists(_k_user(username)):
        raise AuthError("User not found.", 404)
    await r.hset(_k_user(username), mapping={
        "password_hash": await hash_password(new_password),
        "password_changed_at": time.time(),
    })
    await revoke_all_sessions(r, username, keep=keep_session_sha)


# ── login throttling ─────────────────────────────────────────────────────────

async def _reserve_attempt(r, username: str, ip: str) -> tuple[int, int]:
    """Count this attempt BEFORE checking the password; returns (attempts by user, by IP).

    Counting first is what makes the limit hold under parallel requests: 40
    guesses sent at once used to all pass a "fewer than 10 so far?" check before
    any of them was recorded.
    """
    async with r.pipeline(transaction=True) as pipe:
        for key in (f"auth:fail:user:{username}", f"auth:fail:ip:{ip}"):
            pipe.incr(key)
            pipe.expire(key, LOGIN_WINDOW)
        by_user, _, by_ip, _ = await pipe.execute()
    return int(by_user), int(by_ip)


async def authenticate(r, username: str, password: str, ip: str = "unknown") -> User:
    """Check a username/password. Raises AuthError (generic message) on failure."""
    name = (username or "").strip().lower()
    by_user, by_ip = await _reserve_attempt(r, name, ip)
    if by_user > MAX_FAILS_PER_USER or by_ip > MAX_FAILS_PER_IP:
        raise AuthError("Too many failed sign-in attempts. Try again in 15 minutes.", 429)

    h = await r.hgetall(_k_user(name)) if USERNAME_RE.match(name) else {}
    ok = await verify_password(password or "", h.get("password_hash") or _DUMMY_HASH)
    if not h or not ok or h.get("disabled") == "1":
        raise AuthError("Wrong username or password.", 401)   # the attempt stays counted

    # A correct password isn't a failure: clear the user's count, give the IP its attempt back.
    async with r.pipeline(transaction=True) as pipe:
        pipe.delete(f"auth:fail:user:{name}")
        pipe.decr(f"auth:fail:ip:{ip}")
        await pipe.execute()
    return _user_from_hash(h)


# ── sessions ─────────────────────────────────────────────────────────────────

def session_ttl() -> int:
    return max(1, settings.SESSION_TTL_DAYS) * 86400


async def create_session(r, username: str) -> str:
    token = secrets.token_urlsafe(32)
    sha = _sha(token)
    async with r.pipeline(transaction=True) as pipe:
        pipe.set(f"auth:session:{sha}", username, ex=session_ttl())
        pipe.sadd(f"auth:user_sessions:{username}", sha)
        pipe.hset(_k_user(username), "last_login_at", time.time())  # every sign-in path goes through here
        await pipe.execute()
    return token


async def user_for_session(r, token: str) -> Optional[User]:
    if not token:
        return None
    return await _lookup(r, "session", f"auth:session:{_sha(token)}")


async def end_session(r, token: str) -> None:
    if not token:
        return
    sha = _sha(token)
    username = await r.get(f"auth:session:{sha}")
    await r.delete(f"auth:session:{sha}")
    if username:
        await r.srem(f"auth:user_sessions:{username}", sha)


async def revoke_all_sessions(r, username: str, keep: Optional[str] = None) -> None:
    key = f"auth:user_sessions:{username}"
    for sha in await r.smembers(key):
        if sha != keep:
            await r.delete(f"auth:session:{sha}")
            await r.srem(key, sha)


def session_sha(token: str) -> str:
    return _sha(token) if token else ""


# ── API tokens ───────────────────────────────────────────────────────────────

async def create_api_token(r, username: str, name: str) -> tuple[str, dict]:
    name = (name or "").strip()[:60] or "API token"
    token = TOKEN_PREFIX + secrets.token_urlsafe(32)
    sha = _sha(token)
    info = {"id": sha[:12], "username": username, "name": name, "created_at": time.time(), "last_used_at": 0}
    async with r.pipeline(transaction=True) as pipe:
        pipe.hset(f"auth:token:{sha}", mapping=info)
        pipe.sadd(f"auth:user_tokens:{username}", sha)
        await pipe.execute()
    return token, _token_view(info)


def _token_view(h: dict) -> dict:
    return {
        "id": h.get("id"),
        "name": h.get("name"),
        "created_at": float(h.get("created_at") or 0),
        "last_used_at": float(h.get("last_used_at") or 0) or None,
    }


async def list_api_tokens(r, username: str) -> list[dict]:
    out = []
    for sha in await r.smembers(f"auth:user_tokens:{username}"):
        h = await r.hgetall(f"auth:token:{sha}")
        if h:
            out.append(_token_view(h))
    return sorted(out, key=lambda t: t["created_at"], reverse=True)


async def revoke_api_token(r, username: str, token_id: str) -> bool:
    for sha in await r.smembers(f"auth:user_tokens:{username}"):
        if sha.startswith(token_id) and len(token_id) >= 8:
            await r.delete(f"auth:token:{sha}")
            await r.srem(f"auth:user_tokens:{username}", sha)
            return True
    return False


async def rename_api_token(r, username: str, token_id: str, name: str) -> Optional[dict]:
    """Change a token's label. The secret itself never changes."""
    name = (name or "").strip()[:60]
    if not name:
        raise AuthError("Give the token a name.")
    if len(token_id) < 8:
        return None
    for sha in await r.smembers(f"auth:user_tokens:{username}"):
        if sha.startswith(token_id):
            key = f"auth:token:{sha}"
            if not await r.exists(key):
                return None
            await r.hset(key, "name", name)
            return _token_view(await r.hgetall(key))
    return None


async def user_for_api_token(r, token: str) -> Optional[User]:
    if not token or not token.startswith(TOKEN_PREFIX):
        return None
    user = await _lookup(r, "token", f"auth:token:{_sha(token)}")
    if user:
        user.via = "token"
    return user


# Resolve a session or API token to its user in ONE Redis round trip.
# Every authenticated request runs this, so it used to cost three sequential
# calls (credential → user → "last used" write). Nothing is cached: revoking a
# token, ending a session or disabling a user takes effect on the next request.
# "last used" is written at most once per TOKEN_TOUCH_SECONDS, not on every call.
# KEYS[1] credential key   ARGV: kind ("session"|"token"), now, touch interval
LOOKUP_SCRIPT = """
local username
if ARGV[1] == 'token' then
    local t = redis.call('HMGET', KEYS[1], 'username', 'last_used_at')
    username = t[1]
    if username and (tonumber(ARGV[2]) - (tonumber(t[2]) or 0)) >= tonumber(ARGV[3]) then
        redis.call('HSET', KEYS[1], 'last_used_at', ARGV[2])
    end
else
    username = redis.call('GET', KEYS[1])
end
if not username then
    return false
end
return redis.call('HGETALL', 'auth:user:' .. username)
"""
TOKEN_TOUCH_SECONDS = 60
_lookup_script = None


async def _lookup(r, kind: str, key: str) -> Optional[User]:
    global _lookup_script
    if _lookup_script is None:
        _lookup_script = r.register_script(LOOKUP_SCRIPT)
    flat = await _lookup_script(keys=[key], args=[kind, time.time(), TOKEN_TOUCH_SECONDS], client=r)
    if not flat:
        return None
    if isinstance(flat, dict):
        h = flat
    else:
        h = dict(zip(flat[0::2], flat[1::2]))
    if not h.get("username"):
        return None
    user = _user_from_hash(h)
    return None if user.disabled else user


# ── invites ──────────────────────────────────────────────────────────────────

async def create_invite(r, role: str, created_by: str) -> tuple[str, dict]:
    check_role(role)
    token = secrets.token_urlsafe(24)
    sha = _sha(token)
    now = time.time()
    info = {"id": sha[:12], "role": role, "created_by": created_by,
            "created_at": now, "expires_at": now + INVITE_TTL}
    async with r.pipeline(transaction=True) as pipe:
        pipe.hset(f"auth:invite:{sha}", mapping=info)
        pipe.expire(f"auth:invite:{sha}", INVITE_TTL)
        pipe.sadd(K_INVITES, sha)
        await pipe.execute()
    return token, _invite_view(info)


def _invite_view(h: dict) -> dict:
    return {
        "id": h.get("id"),
        "role": h.get("role"),
        "created_by": h.get("created_by"),
        "created_at": float(h.get("created_at") or 0),
        "expires_at": float(h.get("expires_at") or 0),
    }


async def get_invite(r, token: str) -> Optional[dict]:
    h = await r.hgetall(f"auth:invite:{_sha(token or '')}")
    return _invite_view(h) if h else None


async def list_invites(r) -> list[dict]:
    out = []
    for sha in await r.smembers(K_INVITES):
        h = await r.hgetall(f"auth:invite:{sha}")
        if h:
            out.append(_invite_view(h))
        else:
            await r.srem(K_INVITES, sha)  # expired
    return sorted(out, key=lambda i: i["created_at"], reverse=True)


async def revoke_invite(r, invite_id: str) -> bool:
    for sha in await r.smembers(K_INVITES):
        if sha.startswith(invite_id) and len(invite_id) >= 8:
            await r.delete(f"auth:invite:{sha}")
            await r.srem(K_INVITES, sha)
            return True
    return False


async def accept_invite(r, token: str, username: str, password: str) -> User:
    sha = _sha(token or "")
    h = await r.hgetall(f"auth:invite:{sha}")
    if not h:
        raise AuthError("This invite link is invalid or has expired. Ask an admin for a new one.", 404)
    # Validate everything before consuming the invite, so a typo doesn't burn it.
    username = normalize_username(username)
    check_password_rules(password)
    if await r.exists(_k_user(username)):
        raise AuthError(f"The username '{username}' is taken.", 409)
    # Consume atomically: only one person can use a link.
    if not await r.delete(f"auth:invite:{sha}"):
        raise AuthError("This invite link has already been used.", 409)
    await r.srem(K_INVITES, sha)
    return await create_user(r, username, password, h["role"], created_by=h.get("created_by", ""))


# ── first-run setup ──────────────────────────────────────────────────────────

async def setup_required(r) -> bool:
    return settings.auth_enabled and await user_count(r) == 0


async def ensure_setup_code(r) -> Optional[str]:
    """Return the current one-time setup code, creating it if needed (None if setup is done)."""
    if await user_count(r) > 0:
        return None
    code = "-".join(secrets.token_hex(3).upper() for _ in range(3))  # e.g. 3F9A1C-0B7E22-9D41F0
    await r.set(K_SETUP_CODE, code, nx=True)
    return await r.get(K_SETUP_CODE)


def setup_banner(code: str, why: str = "") -> str:
    bar = "=" * 64
    return (f"\n{bar}\n  PyTaskQ first-run setup{(' (' + why + ')') if why else ''}\n\n"
            f"  Open the dashboard and enter this setup code to create the admin:\n\n"
            f"      {code}\n\n"
            f"  (Lost it? Run: pytaskq users setup-code)\n{bar}")


async def announce_setup_code(r, log, why: str = "no accounts exist any more") -> Optional[str]:
    """
    If nobody can sign in and there's no setup code either, make one and log it.

    The code used to be created only when the dashboard started. After Redis
    lost its data (a restart without its volume, a flush, eviction), every
    account was gone and the dashboard asked for a code that didn't exist.
    Safe to call often, and from several API processes: only one creates a code.
    """
    if not settings.auth_enabled or await r.exists(K_SETUP_CODE) or await user_count(r) > 0:
        return None
    code = "-".join(secrets.token_hex(3).upper() for _ in range(3))
    if await r.set(K_SETUP_CODE, code, nx=True):
        log.warning(setup_banner(code, why))
        return code
    return None


async def complete_setup(r, code: str, username: str, password: str) -> User:
    if await user_count(r) > 0:
        raise AuthError("Setup is already complete. Sign in instead.", 409)
    expected = await r.get(K_SETUP_CODE)
    given = (code or "").strip().upper()
    if not expected:
        import logging
        await announce_setup_code(r, logging.getLogger("api"))
        raise AuthError("That code isn't valid any more. A new setup code was just printed in the PyTaskQ server "
                        "log (or run `pytaskq users setup-code` on the server).", 403)
    if not hmac.compare_digest(given, expected):
        raise AuthError("That setup code is not right. Use the newest code in the PyTaskQ server log, or run "
                        "`pytaskq users setup-code` on the server to print it.", 403)
    # Lock so two browsers can't both finish setup at the same moment
    if not await r.set("auth:setup_lock", "1", nx=True, ex=30):
        raise AuthError("Setup is already in progress.", 409)
    try:
        # Check again under the lock: another browser may have just finished.
        if await user_count(r) > 0:
            raise AuthError("Setup is already complete. Sign in instead.", 409)
        user = await create_user(r, username, password, "admin", created_by="setup")
        await r.delete(K_SETUP_CODE)
        return user
    finally:
        await r.delete("auth:setup_lock")


async def bootstrap_from_env(r) -> Optional[User]:
    """Create the first admin from PYTASKQ_ADMIN_USER / PYTASKQ_ADMIN_PASSWORD if no users exist."""
    if not (settings.PYTASKQ_ADMIN_USER and settings.PYTASKQ_ADMIN_PASSWORD):
        return None
    if await user_count(r) > 0:
        return None
    try:
        user = await create_user(r, settings.PYTASKQ_ADMIN_USER, settings.PYTASKQ_ADMIN_PASSWORD,
                                 "admin", created_by="env")
    except AuthError as e:
        if e.status == 409:      # another API process (--workers N) created it a moment ago
            return None
        raise
    await r.delete(K_SETUP_CODE)
    return user


# Pseudo-users for non-interactive access
def local_admin() -> User:
    return User(username="local", role="admin", via="auth_off")


def master_key_user() -> User:
    return User(username="master-key", role="admin", via="master_key")


def matches_master_key(token: str) -> bool:
    key = settings.MASTER_KEY
    return bool(key) and len(key) >= 16 and hmac.compare_digest(token.encode(), key.encode())
