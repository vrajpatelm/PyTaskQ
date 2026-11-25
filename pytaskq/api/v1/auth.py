"""
/auth — sign-in, first-run setup, invites, users and API tokens.

Public:   GET /auth/status, POST /auth/setup, POST /auth/login,
          GET /auth/invites/{token}, POST /auth/invites/{token}/accept
Signed in: GET /auth/me, POST /auth/logout, POST /auth/me/password,
          GET|POST /auth/me/tokens, PATCH|DELETE /auth/me/tokens/{id}
Admin:    GET /auth/users, PATCH|DELETE /auth/users/{username},
          GET|POST /auth/invites, DELETE /auth/invites/{id}
"""

from typing import Any, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from pytaskq.core import auth, redis_client
from pytaskq.core.config import settings
from pytaskq.core.dependencies import SESSION_COOKIE, current_user, get_client_ip, require

router = APIRouter(prefix="/auth", tags=["auth"])

Role = Literal["viewer", "operator", "admin"]


def _fail(e: auth.AuthError):
    raise HTTPException(status_code=e.status, detail=e.message)


def _secure_cookie(request: Request) -> bool:
    mode = settings.COOKIE_SECURE.strip().lower()
    if mode in ("true", "1", "yes", "on"):
        return True
    if mode in ("false", "0", "no", "off"):
        return False
    return request.url.scheme == "https" or request.headers.get("x-forwarded-proto", "").lower() == "https"


def _set_session(response: Response, request: Request, token: str) -> None:
    response.set_cookie(
        SESSION_COOKIE, token,
        max_age=auth.session_ttl(), httponly=True, samesite="lax",
        secure=_secure_cookie(request), path="/",
    )


async def _sign_in(response: Response, request: Request, user: auth.User) -> dict:
    token = await auth.create_session(redis_client.r, user.username)
    _set_session(response, request, token)
    return {"user": user.public()}


# ── public ──────────────────────────────────────────────────────────────────────

class Credentials(BaseModel):
    username: str = Field(..., max_length=64)
    password: str = Field(..., max_length=auth.MAX_PASSWORD)


class SetupRequest(Credentials):
    setup_code: str = Field(..., max_length=64)


@router.get("/status")
async def status(request: Request):
    """What the dashboard should show: setup screen, sign-in screen, or the app."""
    r = redis_client.r
    out: dict[str, Any] = {"auth_enabled": settings.auth_enabled, "setup_required": await auth.setup_required(r), "user": None}
    try:
        out["user"] = (await current_user(request)).public()
    except HTTPException:
        pass
    return out


@router.post("/setup")
async def setup(body: SetupRequest, request: Request, response: Response):
    """Create the first admin account using the setup code from the server log."""
    try:
        user = await auth.complete_setup(redis_client.r, body.setup_code, body.username, body.password)
    except auth.AuthError as e:
        _fail(e)
    return await _sign_in(response, request, user)


@router.post("/login")
async def login(body: Credentials, request: Request, response: Response):
    if not settings.auth_enabled:
        raise HTTPException(status_code=400, detail="Sign-in is turned off on this server (PYTASKQ_AUTH=off).")
    try:
        user = await auth.authenticate(redis_client.r, body.username, body.password, ip=get_client_ip(request))
    except auth.AuthError as e:
        _fail(e)
    return await _sign_in(response, request, user)


@router.get("/invites/{token}")
async def view_invite(token: str):
    """Lets the invite screen show which role the link grants (public)."""
    invite = await auth.get_invite(redis_client.r, token)
    if not invite:
        raise HTTPException(status_code=404, detail="This invite link is invalid or has expired. Ask an admin for a new one.")
    return {"role": invite["role"], "created_by": invite["created_by"], "expires_at": invite["expires_at"]}


@router.post("/invites/{token}/accept")
async def accept_invite(token: str, body: Credentials, request: Request, response: Response):
    try:
        user = await auth.accept_invite(redis_client.r, token, body.username, body.password)
    except auth.AuthError as e:
        _fail(e)
    return await _sign_in(response, request, user)


# ── signed in ───────────────────────────────────────────────────────────────────

@router.post("/logout")
async def logout(request: Request, response: Response):
    await auth.end_session(redis_client.r, request.cookies.get(SESSION_COOKIE, ""))
    response.delete_cookie(SESSION_COOKIE, path="/")
    return {"ok": True}


@router.get("/me")
async def me(user: auth.User = Depends(current_user)):
    return {"user": user.public()}


class PasswordChange(BaseModel):
    current_password: str = Field(..., max_length=auth.MAX_PASSWORD)
    new_password: str = Field(..., max_length=auth.MAX_PASSWORD)


@router.post("/me/password")
async def change_password(body: PasswordChange, request: Request, user: auth.User = Depends(current_user)):
    if user.via != "session":
        raise HTTPException(status_code=400, detail="Change your password from the dashboard while signed in.")
    try:
        await auth.authenticate(redis_client.r, user.username, body.current_password, ip=get_client_ip(request))
    except auth.AuthError as e:
        if e.status == 401:
            raise HTTPException(status_code=400, detail="Your current password is not right.")
        _fail(e)
    keep = auth.session_sha(request.cookies.get(SESSION_COOKIE, ""))
    try:
        await auth.set_password(redis_client.r, user.username, body.new_password, keep_session_sha=keep)
    except auth.AuthError as e:
        _fail(e)
    return {"ok": True, "message": "Password changed. You were signed out everywhere else."}


def _real_user(user: auth.User) -> auth.User:
    if user.via in ("auth_off", "master_key"):
        raise HTTPException(status_code=400, detail="API tokens belong to a user account; sign in as a user first.")
    return user


class TokenRequest(BaseModel):
    name: str = Field("API token", max_length=60)


@router.get("/me/tokens")
async def my_tokens(user: auth.User = Depends(current_user)):
    _real_user(user)
    return {"tokens": await auth.list_api_tokens(redis_client.r, user.username)}


@router.post("/me/tokens")
async def create_token(body: TokenRequest, user: auth.User = Depends(current_user)):
    _real_user(user)
    token, info = await auth.create_api_token(redis_client.r, user.username, body.name)
    return {"token": token, "info": info,
            "message": "Copy this token now — it won't be shown again."}


@router.patch("/me/tokens/{token_id}")
async def rename_token(token_id: str, body: TokenRequest, user: auth.User = Depends(current_user)):
    _real_user(user)
    try:
        info = await auth.rename_api_token(redis_client.r, user.username, token_id, body.name)
    except auth.AuthError as e:
        _fail(e)
    if not info:
        raise HTTPException(status_code=404, detail="Token not found.")
    return {"info": info}


@router.delete("/me/tokens/{token_id}")
async def delete_token(token_id: str, user: auth.User = Depends(current_user)):
    _real_user(user)
    if not await auth.revoke_api_token(redis_client.r, user.username, token_id):
        raise HTTPException(status_code=404, detail="Token not found.")
    return {"ok": True}


# ── admin ───────────────────────────────────────────────────────────────────────

@router.get("/users")
async def users(_: auth.User = Depends(require("admin"))):
    return {"users": [u.public() for u in await auth.list_users(redis_client.r)]}


class UserUpdate(BaseModel):
    role: Optional[Role] = None
    disabled: Optional[bool] = None


@router.patch("/users/{username}")
async def update_user(username: str, body: UserUpdate, admin: auth.User = Depends(require("admin"))):
    try:
        user = await auth.update_user(redis_client.r, username, role=body.role, disabled=body.disabled, acting=admin)
    except auth.AuthError as e:
        _fail(e)
    return {"user": user.public()}


@router.delete("/users/{username}")
async def remove_user(username: str, admin: auth.User = Depends(require("admin"))):
    if username == admin.username:
        raise HTTPException(status_code=400, detail="You can't delete your own account.")
    try:
        await auth.delete_user(redis_client.r, username)
    except auth.AuthError as e:
        _fail(e)
    return {"ok": True}


class InviteRequest(BaseModel):
    role: Role = "viewer"


@router.get("/invites")
async def invites(_: auth.User = Depends(require("admin"))):
    return {"invites": await auth.list_invites(redis_client.r)}


@router.post("/invites")
async def create_invite(body: InviteRequest, admin: auth.User = Depends(require("admin"))):
    token, info = await auth.create_invite(redis_client.r, body.role, created_by=admin.username)
    return {"token": token, "path": f"/?invite={token}", "invite": info}


@router.delete("/invites/{invite_id}")
async def revoke_invite(invite_id: str, _: auth.User = Depends(require("admin"))):
    if not await auth.revoke_invite(redis_client.r, invite_id):
        raise HTTPException(status_code=404, detail="Invite not found (it may have been used or expired).")
    return {"ok": True}
