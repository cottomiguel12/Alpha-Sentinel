from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from jose import jwt
from pydantic import BaseModel, Field

from app.db import db, init_db

# ----------------------------
# Config
# ----------------------------
JWT_SECRET = os.environ.get("JWT_SECRET", "change_me")
JWT_ALG = "HS256"
JWT_EXPIRE_MINUTES = int(os.environ.get("JWT_EXPIRE_MINUTES", "60"))

ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "admin@alpha-sentinel.local")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "sentinel")
ADMIN_ROLE = os.environ.get("ADMIN_ROLE", "sentinel")

MONITOR_MAX = int(os.environ.get("MONITOR_MAX", "10"))

# PBKDF2 settings
PBKDF2_ITER = 200_000
SALT_BYTES = 16


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ----------------------------
# Password hashing (stdlib)
# Format: pbkdf2$iter$salt_b64$dk_b64
# ----------------------------
def _pbkdf2_hash(password: str, iterations: int = PBKDF2_ITER) -> str:
    if password is None:
        password = ""
    salt = secrets.token_bytes(SALT_BYTES)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return "pbkdf2$%d$%s$%s" % (
        iterations,
        base64.b64encode(salt).decode("ascii"),
        base64.b64encode(dk).decode("ascii"),
    )


def _pbkdf2_verify(password: str, stored: str) -> bool:
    try:
        algo, iter_s, salt_b64, dk_b64 = stored.split("$", 3)
        if algo != "pbkdf2":
            return False
        iterations = int(iter_s)
        salt = base64.b64decode(salt_b64.encode("ascii"))
        dk_expected = base64.b64decode(dk_b64.encode("ascii"))
        dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
        return hmac.compare_digest(dk, dk_expected)
    except Exception:
        return False


# ----------------------------
# JWT helpers
# ----------------------------
def _create_token(email: str, role: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=JWT_EXPIRE_MINUTES)
    payload = {"sub": email, "role": role, "exp": int(exp.timestamp())}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)


def _decode_token(token: str) -> Dict[str, Any]:
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])


def require_user(req: Request) -> Dict[str, Any]:
    auth = req.headers.get("authorization") or ""
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = auth.split(" ", 1)[1].strip()
    try:
        claims = _decode_token(token)
        return {"email": claims.get("sub"), "role": claims.get("role")}
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


# ----------------------------
# Pydantic models
# ----------------------------
class LoginIn(BaseModel):
    email: str
    password: str


class AOIIn(BaseModel):
    # Either provide contract_key OR (ticker/exp/strike/opt_type)
    contract_key: Optional[str] = None
    ticker: Optional[str] = None
    exp: Optional[str] = None  # YYYY-MM-DD
    strike: Optional[float] = None
    opt_type: Optional[str] = None  # C/P
    notes: Optional[str] = None


class ToggleIn(BaseModel):
    contract_key: str
    is_active: int = Field(..., ge=0, le=1)


# ----------------------------
# App lifespan (init DB + admin upsert)
# ----------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    # Upsert admin user (idempotent)
    with db() as conn:
        row = conn.execute("SELECT id,password_hash,role,is_active FROM users WHERE email=?", (ADMIN_EMAIL,)).fetchone()
        if not row:
            conn.execute(
                """
                INSERT INTO users (email,password_hash,role,is_active,created_at,last_login_at)
                VALUES (?,?,?,?,?,?)
                """,
                (ADMIN_EMAIL, _pbkdf2_hash(ADMIN_PASSWORD), ADMIN_ROLE, 1, now_iso(), None),
            )
        else:
            # Always sync admin password and role during startup so env holds priority
            conn.execute(
                "UPDATE users SET password_hash=?, role=?, is_active=1 WHERE email=?", 
                (_pbkdf2_hash(ADMIN_PASSWORD), ADMIN_ROLE, ADMIN_EMAIL)
            )

    yield


APP = FastAPI(title="Alpha Sentinel API", lifespan=lifespan)


@APP.exception_handler(Exception)
async def on_exception(_req: Request, exc: Exception):
    # helpful JSON error in logs
    return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})


# ----------------------------
# Basic endpoints
# ----------------------------
@APP.get("/health")
async def health():
    return {"ok": True, "ts": now_iso()}


@APP.post("/auth/login")
async def login(body: LoginIn):
    with db() as conn:
        row = conn.execute("SELECT * FROM users WHERE email=?", (body.email.strip().lower(),)).fetchone()
        if not row or not int(row["is_active"]):
            raise HTTPException(status_code=401, detail="Invalid credentials")

        if not _pbkdf2_verify(body.password, row["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid credentials")

        conn.execute("UPDATE users SET last_login_at=? WHERE email=?", (now_iso(), row["email"]))
        token = _create_token(row["email"], row["role"])
        return {"ok": True, "token": token, "email": row["email"], "role": row["role"]}


# ----------------------------
# Alerts (dashboard priority)
# ----------------------------
def _format_alerts(rows, conn):
    cks = []
    for r in rows:
        d = dict(r)
        exp = (d.get("exp") or "").strip()
        if exp:
            exp = exp.split("T")[0].split(" ")[0]
        ot = (d.get("opt_type") or "").strip().upper()
        ot = "C" if ot.startswith("C") else "P" if ot.startswith("P") else ot
        strike = d.get("strike")
        try:
            strike = float(strike) if strike is not None else None
        except Exception:
            strike = None

        if d.get("ticker") and exp and strike is not None and ot:
            ck = _make_contract_key(d["ticker"], exp, strike, ot)
            cks.append(ck)

    active = set()
    if cks:
        qmarks = ",".join(["?"] * len(cks))
        active_rows = conn.execute(
            f"SELECT contract_key FROM watchlist WHERE is_active=1 AND contract_key IN ({qmarks})",
            tuple(cks),
        ).fetchall()
        active = {r["contract_key"] for r in active_rows}

    items = []
    for r in rows:
        d = dict(r)
        
        exp = (d.get("exp") or "").strip()
        if exp:
            exp = exp.split("T")[0].split(" ")[0]
        d["exp"] = exp or d.get("exp")

        ot = (d.get("opt_type") or "").strip().upper()
        ot = "C" if ot.startswith("C") else "P" if ot.startswith("P") else ot
        d["opt_type"] = ot or d.get("opt_type")

        try:
            if d.get("strike") is not None:
                d["strike"] = float(d["strike"])
        except Exception:
            pass

        if d.get("ticker") and d.get("exp") and d.get("strike") is not None and d.get("opt_type"):
            d["contract_key"] = _make_contract_key(d["ticker"], d["exp"], float(d["strike"]), d["opt_type"])
        else:
            d["contract_key"] = d.get("contract_key")

        d["is_aoi"] = 1 if d.get("contract_key") in active else 0

        for k in ("reason_codes",):
            if k in d and isinstance(d[k], str):
                try:
                    d[k] = json.loads(d[k])
                except Exception:
                    pass

        items.append(d)
    return items

@APP.get("/alerts")
async def alerts(
    limit: int = 50,
    symbol: Optional[str] = None,
    type: Optional[str] = None,
    min_premium: Optional[float] = None,
    dte_min: Optional[int] = None,
    dte_max: Optional[int] = None,
    sort_score: Optional[str] = None,
    user=Depends(require_user)
):
    limit = max(1, min(int(limit), 500))

    query = "SELECT * FROM alerts"
    conditions = []
    params = []

    if symbol:
        conditions.append("ticker = ?")
        params.append(symbol.strip().upper())
    if type:
        ot = type.strip().upper()
        ot = "C" if ot.startswith("C") else "P" if ot.startswith("P") else ot
        conditions.append("opt_type = ?")
        params.append(ot)
    if min_premium is not None:
        conditions.append("premium >= ?")
        params.append(min_premium)
    if dte_min is not None:
        conditions.append("dte >= ?")
        params.append(dte_min)
    if dte_max is not None:
        conditions.append("dte <= ?")
        params.append(dte_max)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    if sort_score and sort_score.lower() == "desc":
        query += " ORDER BY score_total DESC LIMIT ?"
    elif sort_score and sort_score.lower() == "asc":
        query += " ORDER BY score_total ASC LIMIT ?"
    else:
        query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    with db() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        items = _format_alerts(rows, conn)

    return {"ok": True, "items": items}

@APP.get("/alerts/recent")
async def alerts_recent(window_sec: int = 900, limit: int = 15, user=Depends(require_user)):
    limit = max(1, min(int(limit), 100))
    cutoff_dt = datetime.now(timezone.utc) - timedelta(seconds=window_sec)
    cutoff_iso = cutoff_dt.isoformat()

    with db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM alerts
            WHERE ts >= ?
            ORDER BY score_total DESC
            LIMIT ?
            """,
            (cutoff_iso, limit),
        ).fetchall()
        items = _format_alerts(rows, conn)

    return {"ok": True, "items": items}


# ----------------------------
# Monitor (top 10)
# ----------------------------
@APP.get("/monitors")
@APP.get("/monitor")
async def monitor(user=Depends(require_user)):
    with db() as conn:
        rows = conn.execute(
            """
            SELECT m.*,
                   1 AS is_aoi
            FROM monitor m
            INNER JOIN watchlist w
              ON w.contract_key = m.contract_key
            WHERE w.is_active = 1
            ORDER BY m.current_score DESC
            LIMIT ?
            """,
            (MONITOR_MAX,),
        ).fetchall()

    out = []
    for r in rows:
        d = dict(r)
        # normalize history + derived fields
        try:
            d["score_history"] = json.loads(d.get("score_history") or "[]")
        except Exception:
            d["score_history"] = []
        try:
            d["delta_from_peak"] = round(float(d.get("peak_score", 0)) - float(d.get("current_score", 0)), 1)
        except Exception:
            d["delta_from_peak"] = None

        # ensure is_aoi is int 0/1
        d["is_aoi"] = int(d.get("is_aoi") or 0)
        out.append(d)
    return {"ok": True, "items": out}


# ----------------------------
# Watchlist (AOI)
# ----------------------------
@APP.get("/watchlist")
async def get_watchlist(user=Depends(require_user)):
    with db() as conn:
        rows = conn.execute(
            """
            SELECT contract_key, added_by, created_at, is_active, notes
            FROM watchlist
            ORDER BY created_at DESC
            """
        ).fetchall()
    return {"ok": True, "items": [dict(r) for r in rows]}


def _make_contract_key(ticker: str, exp: str, strike: float, opt_type: str) -> str:
    t = (ticker or "").strip().upper()
    e = (exp or "").strip().split("T")[0].split(" ")[0]
    ot = (opt_type or "").strip().upper()
    if ot.startswith("C"):
        ot = "C"
    elif ot.startswith("P"):
        ot = "P"
    else:
        ot = "C"
    return f"{t}|{e}|{float(strike)}|{ot}"


@APP.post("/aoi")
async def add_aoi(body: AOIIn, user=Depends(require_user)):
    # build contract_key
    if body.contract_key:
        ck = body.contract_key.strip()
    else:
        if not (body.ticker and body.exp and body.strike is not None and body.opt_type):
            raise HTTPException(status_code=400, detail="Provide contract_key OR ticker/exp/strike/opt_type")
        ck = _make_contract_key(body.ticker, body.exp, float(body.strike), body.opt_type)

    with db() as conn:
        conn.execute(
            """
            INSERT INTO watchlist (contract_key, added_by, created_at, is_active, notes)
            VALUES (?,?,?,?,?)
            ON CONFLICT(contract_key) DO UPDATE SET
              is_active=1,
              notes=COALESCE(excluded.notes, watchlist.notes)
            """,
            (ck, user["email"], now_iso(), 1, body.notes),
        )
    return {"ok": True, "contract_key": ck}


@APP.post("/aoi/from_alert/{alert_id}")
async def add_aoi_from_alert(alert_id: int, notes: Optional[str] = None, user=Depends(require_user)):
    with db() as conn:
        r = conn.execute("SELECT * FROM alerts WHERE id=?", (int(alert_id),)).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="Alert not found")

        d = dict(r)
        ck = d.get("contract_key")
        if not ck:
            ck = _make_contract_key(d["ticker"], d["exp"], float(d["strike"]), d["opt_type"])

        conn.execute(
            """
            INSERT INTO watchlist (contract_key, added_by, created_at, is_active, notes)
            VALUES (?,?,?,?,?)
            ON CONFLICT(contract_key) DO UPDATE SET
              is_active=1,
              notes=COALESCE(excluded.notes, watchlist.notes)
            """,
            (ck, user["email"], now_iso(), 1, notes),
        )
    return {"ok": True, "contract_key": ck, "from_alert": alert_id}


@APP.post("/watchlist/toggle")
async def toggle_watchlist(body: ToggleIn, user=Depends(require_user)):
    with db() as conn:
        conn.execute(
            """
            INSERT INTO watchlist (contract_key, added_by, created_at, is_active, notes)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(contract_key) DO UPDATE SET
              is_active=?
            """,
            (body.contract_key, user["email"], now_iso(), int(body.is_active), "", int(body.is_active)),
        )
    return {"ok": True, "contract_key": body.contract_key, "is_active": int(body.is_active)}


@APP.delete("/watchlist/{contract_key}")
async def delete_watchlist(contract_key: str, user=Depends(require_user)):
    with db() as conn:
        conn.execute("DELETE FROM watchlist WHERE contract_key=?", (contract_key,))
    return {"ok": True, "deleted": contract_key}


@APP.post("/admin/purge-mock")
async def purge_mock(user=Depends(require_user)):
    with db() as conn:
        result = conn.execute("DELETE FROM alerts WHERE tags = 'MOCK' OR reason_codes LIKE '%MOCK_DATA%'")
        deleted = result.rowcount
    return {"ok": True, "deleted": deleted}


# ----------------------------
# Integrations status endpoints
# ----------------------------

@APP.get("/integrations")
async def integrations(user=Depends(require_user)):
    uw_enabled = os.environ.get("UW_ENABLED", "0").strip() in ("1", "true", "yes")
    uw_api_key = os.environ.get("UW_API_KEY", "").strip()
    uw_base_url = os.environ.get("UW_BASE_URL", "https://api.unusualwhales.com")
    uw_mode = os.environ.get("UW_MODE", "poll")
    return {
        "ok": True,
        "unusual_whales": {
            "enabled": uw_enabled,
            "configured": bool(uw_api_key),
            "mode": uw_mode,
            "base_url": uw_base_url,
            "coming_soon": not uw_enabled,
        },
    }


@APP.get("/uw/health")
async def uw_health(user=Depends(require_user)):
    uw_enabled = os.environ.get("UW_ENABLED", "0").strip() in ("1", "true", "yes")
    uw_api_key = os.environ.get("UW_API_KEY", "").strip()
    if not uw_enabled:
        return {"ok": True, "enabled": False, "message": "UW integration is disabled (UW_ENABLED=0)"}
    if not uw_api_key:
        return {"ok": False, "enabled": True, "error": "missing api key — set UW_API_KEY"}
    # UW is enabled and configured — live health check not yet implemented
    return {"ok": True, "enabled": True, "configured": True, "message": "UW configured but live health check not yet implemented"}


# ----------------------------
# Entrypoint
# ----------------------------
def main():
    # keep uvicorn import inside so "python -m app.api" works
    import uvicorn

    port = int(os.environ.get("API_PORT", "8001"))
    uvicorn.run("app.api:APP", host="0.0.0.0", port=port, log_level="info")


if __name__ == "__main__":
    main()