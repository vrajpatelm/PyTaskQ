"""
test_auth.py — self-hosted sign-in, roles, invites and API tokens
==================================================================

Every rule the auth system promises is pinned down here:
  * first-run setup needs the setup code from the server log
  * sign-in, throttling, generic error messages
  * the viewer / operator / admin permission matrix on real endpoints
  * cross-site write protection for cookie sessions
  * API tokens and the master key
  * invite links, team management, the "last admin" guard
  * password changes sign out other sessions
  * WebSocket authentication
  * the `pytaskq users` CLI
"""

import asyncio
from unittest.mock import patch

import httpx
import pytest

from pytaskq.core import auth
from pytaskq.core.config import settings

PASSWORD = "correct horse battery"


@pytest.fixture
async def env(r):
    from pytaskq.main import app
    with patch("pytaskq.core.redis_client.r", r), patch.object(settings, "PYTASKQ_AUTH", "on"), \
         patch.object(settings, "MASTER_KEY", ""):
        clients = []

        def new_client(**headers):
            c = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers=headers)
            clients.append(c)
            return c

        yield r, new_client
        for c in clients:
            await c.aclose()


async def signed_in(r, new_client, username, role):
    await auth.create_user(r, username, PASSWORD, role)
    c = new_client()
    res = await c.post("/auth/login", json={"username": username, "password": PASSWORD})
    assert res.status_code == 200, res.text
    return c


# ── first-run setup ─────────────────────────────────────────────────────────────

async def test_first_run_setup_with_code(env):
    r, new_client = env
    c = new_client()
    status = (await c.get("/auth/status")).json()
    assert status == {"auth_enabled": True, "setup_required": True, "user": None}

    code = await auth.ensure_setup_code(r)
    assert code == await auth.ensure_setup_code(r), "the code is stable until used"

    bad = await c.post("/auth/setup", json={"setup_code": "WRONG", "username": "vraj", "password": PASSWORD})
    assert bad.status_code == 403
    assert await auth.user_count(r) == 0

    ok = await c.post("/auth/setup", json={"setup_code": code.lower(), "username": "Vraj", "password": PASSWORD})
    assert ok.status_code == 200, ok.text
    assert ok.json()["user"]["username"] == "vraj"
    assert ok.json()["user"]["role"] == "admin"
    cookie = ok.headers["set-cookie"].lower()
    assert "httponly" in cookie and "samesite=lax" in cookie

    me = (await c.get("/auth/me")).json()["user"]
    assert me["username"] == "vraj", "setup signs you in"
    assert me["last_login_at"] > 0, "joining counts as a sign-in"
    assert not await r.exists("auth:setup_code"), "the code is single-use"
    again = await new_client().post("/auth/setup", json={"setup_code": code, "username": "x" * 3, "password": PASSWORD})
    assert again.status_code == 409


async def test_setup_validates_before_consuming_code(env):
    r, new_client = env
    code = await auth.ensure_setup_code(r)
    res = await new_client().post("/auth/setup", json={"setup_code": code, "username": "vraj", "password": "short"})
    assert res.status_code == 400
    assert await r.get("auth:setup_code") == code


async def test_bootstrap_admin_from_env(env):
    r, _ = env
    with patch.object(settings, "PYTASKQ_ADMIN_USER", "ops"), patch.object(settings, "PYTASKQ_ADMIN_PASSWORD", PASSWORD):
        user = await auth.bootstrap_from_env(r)
        assert user.role == "admin"
        assert await auth.bootstrap_from_env(r) is None, "only when no users exist"
    assert await auth.ensure_setup_code(r) is None


# ── sign-in ─────────────────────────────────────────────────────────────────────

async def test_login_errors_are_generic(env):
    r, new_client = env
    await auth.create_user(r, "ada", PASSWORD, "viewer")
    c = new_client()
    wrong_pw = await c.post("/auth/login", json={"username": "ada", "password": "nope-nope"})
    no_user = await c.post("/auth/login", json={"username": "ghost", "password": "nope-nope"})
    assert wrong_pw.status_code == no_user.status_code == 401
    assert wrong_pw.json() == no_user.json(), "don't reveal which usernames exist"


async def test_login_is_throttled(env):
    r, new_client = env
    await auth.create_user(r, "ada", PASSWORD, "viewer")
    c = new_client()
    for _ in range(auth.MAX_FAILS_PER_USER):
        assert (await c.post("/auth/login", json={"username": "ada", "password": "wrong-pass"})).status_code == 401
    blocked = await c.post("/auth/login", json={"username": "ada", "password": PASSWORD})
    assert blocked.status_code == 429
    await r.delete("auth:fail:user:ada")
    assert (await c.post("/auth/login", json={"username": "ada", "password": PASSWORD})).status_code == 200


async def test_passwords_and_sessions_are_stored_hashed(env):
    r, new_client = env
    c = await signed_in(r, new_client, "ada", "viewer")
    stored = await r.hget("auth:user:ada", "password_hash")
    assert stored.startswith("scrypt$") and PASSWORD not in stored
    token = c.cookies.get("pytaskq_session")
    assert token and not await r.exists(f"auth:session:{token}"), "raw session token never stored"


async def test_logout(env):
    r, new_client = env
    c = await signed_in(r, new_client, "ada", "viewer")
    assert (await c.post("/auth/logout")).status_code == 200
    assert (await c.get("/tasks")).status_code == 401


async def test_disabled_user_is_signed_out_and_blocked(env):
    r, new_client = env
    admin = await signed_in(r, new_client, "boss", "admin")
    ada = await signed_in(r, new_client, "ada", "viewer")
    assert (await admin.patch("/auth/users/ada", json={"disabled": True})).status_code == 200
    assert (await ada.get("/tasks")).status_code == 401
    res = await new_client().post("/auth/login", json={"username": "ada", "password": PASSWORD})
    assert res.status_code == 401


# ── permissions ─────────────────────────────────────────────────────────────────

async def test_anonymous_requests_are_rejected_but_public_pages_work(env):
    _, new_client = env
    c = new_client()
    for method, path in [("GET", "/tasks"), ("GET", "/task/x"), ("GET", "/metrics"), ("GET", "/dlq"),
                         ("GET", "/system/workers"), ("POST", "/task/enqueue"), ("POST", "/dlq/purge_all"),
                         ("GET", "/auth/users")]:
        res = await c.request(method, path, json={"task_name": "x"} if method == "POST" else None)
        assert res.status_code == 401, (method, path, res.status_code)
    assert (await c.get("/health")).status_code == 200
    assert (await c.get("/")).status_code == 200
    assert (await c.get("/auth/status")).status_code == 200


@pytest.mark.parametrize("role, can_read, can_write, can_admin", [
    ("viewer", True, False, False),
    ("operator", True, True, False),
    ("admin", True, True, True),
])
async def test_role_matrix(env, role, can_read, can_write, can_admin):
    r, new_client = env
    c = await signed_in(r, new_client, f"u-{role}", role)
    expect = lambda ok: 200 if ok else 403  # noqa: E731

    assert (await c.get("/tasks")).status_code == expect(can_read)
    assert (await c.get("/metrics")).status_code == expect(can_read)
    assert (await c.get("/dlq")).status_code == expect(can_read)

    enq = await c.post("/task/enqueue", json={"task_name": "x"})
    assert enq.status_code == expect(can_write)
    assert (await c.post("/dlq/purge_all")).status_code == expect(can_write)
    assert (await c.post("/dlq/retry_all")).status_code == expect(can_write)
    if can_write:
        tid = enq.json()["task_id"]
        assert (await c.post(f"/task/{tid}/cancel")).status_code == 200
    else:
        assert "operator role" in enq.json()["detail"]

    assert (await c.get("/auth/users")).status_code == expect(can_admin)
    assert (await c.post("/auth/invites", json={"role": "viewer"})).status_code == expect(can_admin)


async def test_jobs_record_who_queued_and_cancelled(env):
    r, new_client = env
    c = await signed_in(r, new_client, "ada", "operator")
    tid = (await c.post("/task/enqueue", json={"task_name": "x"})).json()["task_id"]
    await c.post(f"/task/{tid}/cancel")
    info = (await c.get(f"/task/{tid}")).json()["result"]
    assert info["created_by"] == "ada" and info["cancelled_by"] == "ada"


async def test_cross_site_writes_are_blocked(env):
    r, new_client = env
    c = await signed_in(r, new_client, "ada", "operator")
    evil = await c.post("/task/enqueue", json={"task_name": "x"}, headers={"Origin": "https://evil.example"})
    assert evil.status_code == 403
    same = await c.post("/task/enqueue", json={"task_name": "x"}, headers={"Origin": "http://test"})
    assert same.status_code == 200
    dev_port = await c.post("/task/enqueue", json={"task_name": "x"}, headers={"Origin": "http://test:5173"})
    assert dev_port.status_code == 200, "same host, different port (Vite dev server) is allowed"
    # Reads are never blocked by the origin check
    assert (await c.get("/tasks", headers={"Origin": "https://evil.example"})).status_code == 200


# ── API tokens & master key ─────────────────────────────────────────────────────

async def test_api_tokens(env):
    r, new_client = env
    c = await signed_in(r, new_client, "ada", "operator")
    created = (await c.post("/auth/me/tokens", json={"name": "deploy script"})).json()
    token = created["token"]
    assert token.startswith("ptq_")

    bot = new_client(Authorization=f"Bearer {token}")
    assert (await bot.post("/task/enqueue", json={"task_name": "x"})).status_code == 200, "inherits operator"
    assert (await bot.get("/auth/users")).status_code == 403
    assert (await bot.post("/auth/me/password", json={"current_password": PASSWORD, "new_password": "x" * 9})).status_code == 400

    listed = (await c.get("/auth/me/tokens")).json()["tokens"]
    assert listed[0]["name"] == "deploy script" and listed[0]["last_used_at"]
    assert "token" not in listed[0], "the secret is only shown once"

    assert (await c.delete(f"/auth/me/tokens/{created['info']['id']}")).status_code == 200
    assert (await bot.get("/tasks")).status_code == 401
    assert (await new_client(Authorization="Bearer ptq_made_up").get("/tasks")).status_code == 401


async def test_token_stops_working_when_owner_is_disabled(env):
    r, new_client = env
    admin = await signed_in(r, new_client, "boss", "admin")
    c = await signed_in(r, new_client, "ada", "operator")
    token = (await c.post("/auth/me/tokens", json={})).json()["token"]
    await admin.patch("/auth/users/ada", json={"disabled": True})
    assert (await new_client(Authorization=f"Bearer {token}").get("/tasks")).status_code == 401


async def test_master_key(env):
    _, new_client = env
    key = "m" * 32
    with patch.object(settings, "MASTER_KEY", key):
        assert (await new_client(Authorization=f"Bearer {key}").get("/auth/users")).status_code == 200
    with patch.object(settings, "MASTER_KEY", "short"):
        assert (await new_client(Authorization="Bearer short").get("/tasks")).status_code == 401, \
            "keys under 16 characters are ignored"


# ── invites ─────────────────────────────────────────────────────────────────────

async def test_invite_flow(env):
    r, new_client = env
    admin = await signed_in(r, new_client, "boss", "admin")
    inv = (await admin.post("/auth/invites", json={"role": "operator"})).json()
    token = inv["token"]
    assert inv["path"] == f"/?invite={token}"

    guest = new_client()
    view = (await guest.get(f"/auth/invites/{token}")).json()
    assert view["role"] == "operator" and view["created_by"] == "boss"

    bad = await guest.post(f"/auth/invites/{token}/accept", json={"username": "boss", "password": PASSWORD})
    assert bad.status_code == 409, "taken username"
    weak = await guest.post(f"/auth/invites/{token}/accept", json={"username": "lin", "password": "short"})
    assert weak.status_code == 400
    assert (await guest.get(f"/auth/invites/{token}")).status_code == 200, "mistakes don't burn the link"

    ok = await guest.post(f"/auth/invites/{token}/accept", json={"username": "lin", "password": PASSWORD})
    assert ok.status_code == 200 and ok.json()["user"]["role"] == "operator"
    assert (await guest.post("/task/enqueue", json={"task_name": "x"})).status_code == 200, "signed in right away"
    assert (await guest.get(f"/auth/invites/{token}")).status_code == 404, "single use"
    assert (await admin.get("/auth/invites")).json()["invites"] == []


async def test_revoke_invite(env):
    r, new_client = env
    admin = await signed_in(r, new_client, "boss", "admin")
    inv = (await admin.post("/auth/invites", json={"role": "viewer"})).json()
    assert len((await admin.get("/auth/invites")).json()["invites"]) == 1
    assert (await admin.delete(f"/auth/invites/{inv['invite']['id']}")).status_code == 200
    accept = await new_client().post(f"/auth/invites/{inv['token']}/accept", json={"username": "lin", "password": PASSWORD})
    assert accept.status_code == 404


# ── team management ─────────────────────────────────────────────────────────────

async def test_last_admin_is_protected(env):
    r, new_client = env
    admin = await signed_in(r, new_client, "boss", "admin")
    assert (await admin.patch("/auth/users/boss", json={"role": "viewer"})).status_code == 409
    assert (await admin.patch("/auth/users/boss", json={"disabled": True})).status_code == 409
    assert (await admin.delete("/auth/users/boss")).status_code == 400

    await auth.create_user(r, "second", PASSWORD, "admin")
    assert (await admin.patch("/auth/users/boss", json={"role": "operator"})).status_code == 200


async def test_admin_manages_users(env):
    r, new_client = env
    admin = await signed_in(r, new_client, "boss", "admin")
    await auth.create_user(r, "ada", PASSWORD, "viewer")
    names = [u["username"] for u in (await admin.get("/auth/users")).json()["users"]]
    assert names == ["ada", "boss"]
    assert "password_hash" not in str((await admin.get("/auth/users")).json())

    up = await admin.patch("/auth/users/ada", json={"role": "operator"})
    assert up.json()["user"]["role"] == "operator"
    assert (await admin.patch("/auth/users/ada", json={"role": "superuser"})).status_code == 422
    assert (await admin.delete("/auth/users/ada")).status_code == 200
    assert (await admin.delete("/auth/users/ada")).status_code == 404


# ── password change ─────────────────────────────────────────────────────────────

async def test_change_password_signs_out_other_sessions(env):
    r, new_client = env
    laptop = await signed_in(r, new_client, "ada", "viewer")
    phone = new_client()
    await phone.post("/auth/login", json={"username": "ada", "password": PASSWORD})

    wrong = await laptop.post("/auth/me/password", json={"current_password": "nope-nope", "new_password": "brand new pass"})
    assert wrong.status_code == 400
    ok = await laptop.post("/auth/me/password", json={"current_password": PASSWORD, "new_password": "brand new pass"})
    assert ok.status_code == 200

    assert (await laptop.get("/auth/me")).status_code == 200, "this session stays"
    assert (await phone.get("/auth/me")).status_code == 401, "other sessions end"
    res = await new_client().post("/auth/login", json={"username": "ada", "password": "brand new pass"})
    assert res.status_code == 200


# ── auth turned off ─────────────────────────────────────────────────────────────

async def test_auth_off_allows_everything(env):
    r, new_client = env
    with patch.object(settings, "PYTASKQ_AUTH", "off"):
        c = new_client()
        st = (await c.get("/auth/status")).json()
        assert st["auth_enabled"] is False and st["setup_required"] is False
        assert st["user"]["username"] == "local"
        assert (await c.post("/task/enqueue", json={"task_name": "x"})).status_code == 200
        assert (await c.get("/auth/users")).status_code == 200


# ── WebSocket ───────────────────────────────────────────────────────────────────

class FakeWS:
    def __init__(self, headers=None, cookies=None, query=None):
        self.headers = {"host": "test", **(headers or {})}
        self.cookies = cookies or {}
        self.query_params = query or {}
        self.closed_with = None
        self.accepted = False

    async def close(self, code=1000):
        self.closed_with = code

    async def accept(self):
        self.accepted = True
        raise asyncio.CancelledError  # stop the handler right after the auth check


async def _ws(ws):
    from pytaskq.api.v1.dashboard import dashboard_websocket
    try:
        await dashboard_websocket(ws)
    except asyncio.CancelledError:
        pass
    return ws


async def test_websocket_requires_sign_in(env):
    r, new_client = env
    anon = await _ws(FakeWS())
    assert anon.closed_with == 4401 and not anon.accepted

    c = await signed_in(r, new_client, "ada", "viewer")
    cookie = {"pytaskq_session": c.cookies.get("pytaskq_session")}
    ok = await _ws(FakeWS(headers={"origin": "http://test"}, cookies=cookie))
    assert ok.accepted

    hijack = await _ws(FakeWS(headers={"origin": "https://evil.example"}, cookies=cookie))
    assert hijack.closed_with == 4401 and not hijack.accepted

    token = (await c.post("/auth/me/tokens", json={})).json()["token"]
    script = await _ws(FakeWS(query={"token": token}))
    assert script.accepted


# ── CLI ─────────────────────────────────────────────────────────────────────────

def test_cli_user_commands(capsys):
    import fakeredis
    import fakeredis.aioredis as fake_aio
    from pytaskq import cli

    server = fakeredis.FakeServer()
    with patch("pytaskq.core.redis_client.make_client", lambda: fake_aio.FakeRedis(server=server, decode_responses=True)):
        cli.main(["users", "setup-code"])
        assert len(capsys.readouterr().out.strip()) == 20  # XXXXXX-XXXXXX-XXXXXX

        cli.main(["users", "create", "Ops", "--role", "admin", "--password", PASSWORD])
        cli.main(["users", "create", "ada", "--password", PASSWORD])
        cli.main(["users", "set-role", "ada", "operator"])
        cli.main(["users", "reset-password", "ada", "--password", "another password"])
        cli.main(["users", "list"])
        out = capsys.readouterr().out
        assert "Created admin 'ops'" in out and "'ada' is now operator" in out
        assert "ops" in out and "operator" in out

        cli.main(["users", "setup-code"])
        assert "already complete" in capsys.readouterr().out

        with pytest.raises(SystemExit):
            cli.main(["users", "delete", "ops"])  # last admin
        cli.main(["users", "delete", "ada"])
        assert "Deleted 'ada'" in capsys.readouterr().out

        async def check():
            r = fake_aio.FakeRedis(server=server, decode_responses=True)
            users = await auth.list_users(r)
            await r.aclose()
            return [u.username for u in users]

        assert asyncio.run(check()) == ["ops"]


# ── unit: hashing & validation ──────────────────────────────────────────────────

async def test_password_hash_roundtrip():
    h = await auth.hash_password("pässwörd-ünïcode")
    assert await auth.verify_password("pässwörd-ünïcode", h)
    assert not await auth.verify_password("password-unicode", h)
    assert h != await auth.hash_password("pässwörd-ünïcode"), "random salt per hash"
    assert not await auth.verify_password("x", "garbage")


@pytest.mark.parametrize("name", ["a", "-bad", "has space", "UPPER!", "x" * 33, ""])
def test_bad_usernames(name):
    with pytest.raises(auth.AuthError):
        auth.normalize_username(name)


def test_username_normalised():
    assert auth.normalize_username("  Vraj.Patel ") == "vraj.patel"
