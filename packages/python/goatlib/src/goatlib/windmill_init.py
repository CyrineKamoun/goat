"""Idempotent windmill workspace + token + password bootstrap.

Pure stdlib (urllib + json). Returns the minted API token to the caller;
the caller decides what to do with it (file, K8s Secret, vault, etc.).

Usage as a library:
    from goatlib.windmill_init import bootstrap
    token = bootstrap(
        url="http://windmill-server",
        workspace="goat",
        admin_email="admin@windmill.dev",
        desired_password="...",
    )

Usage as a CLI (prints the token as the last line of stdout):
    python -m goatlib.windmill_init

This module is the canonical place for windmill bootstrap logic; future
consumers (compose's `scripts/windmill/init-windmill.py`, the
plan4better/charts windmill bootstrap hook, civitas-goat-addon, etc.)
should call into it rather than re-implementing the same login/workspace/
token-mint flow.

Placed at the top level of goatlib (rather than under auth/ or services/)
because windmill is its own domain — neither auth-the-way-Keycloak-is
nor an external object-storage service. Parallel to
goatlib.storage.ducklake_init.
"""

from __future__ import annotations

import json
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

_DEFAULT_TIMEOUT_SECONDS = 30
_DEFAULT_WAIT_MAX_SECONDS = 120
_DEFAULT_WAIT_INTERVAL_SECONDS = 2


def bootstrap(
    url: str,
    workspace: str,
    admin_email: str,
    desired_password: str,
    default_password: str = "changeme",
    token_label: str = "goatlib-bootstrap",
    timeout_seconds: int = _DEFAULT_TIMEOUT_SECONDS,
    wait_max_seconds: int = _DEFAULT_WAIT_MAX_SECONDS,
    wait_interval_seconds: int = _DEFAULT_WAIT_INTERVAL_SECONDS,
) -> str:
    """Bootstrap a windmill workspace and mint an API token.

    Idempotent at the workspace + password level. The token mint is *not*
    idempotent — each call creates a new token row in windmill's tokens
    table; old tokens remain valid until revoked separately. That matches
    upstream init scripts' behaviour and is expected by the consumers that
    just need a fresh token written out each install.

    Steps:
        1. Wait for `{url}/api/version` to return 200 (up to wait_max_seconds)
        2. Try login with desired_password
        3. If that fails, try default_password; if it succeeds, rotate the
           password to desired_password and re-login
        4. Create the workspace if missing (swallow "already exists" errors)
        5. Mint a non-expiring API token with the given label

    Args:
        url: Windmill base URL (e.g. "http://windmill-server").
        workspace: Workspace ID to ensure exists.
        admin_email: Superadmin login email.
        desired_password: Password to set / log in with.
        default_password: Windmill's first-boot superadmin password.
        token_label: Human-readable label for the minted token.
        timeout_seconds: Per-request HTTP timeout.
        wait_max_seconds: Max time to wait for windmill API to come up.
        wait_interval_seconds: Poll interval while waiting.

    Returns:
        The minted API token string.

    Raises:
        RuntimeError: If windmill never becomes ready, both passwords fail
            to log in, password rotation re-login fails, or the token mint
            API doesn't return a token.
    """
    base = url.rstrip("/")

    def _api(path, method="GET", body=None, token=None, json_resp=True):
        req = Request(f"{base}/api{path}", method=method)
        req.add_header("Content-Type", "application/json")
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        data = json.dumps(body).encode() if body is not None else None
        with urlopen(req, data=data, timeout=timeout_seconds) as r:
            raw = r.read().decode()
        if not raw:
            return {} if json_resp else ""
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw.strip()

    def _try_login(email: str, password: str) -> str | None:
        try:
            res = _api(
                "/auth/login",
                method="POST",
                body={"email": email, "password": password},
                json_resp=False,
            )
            return res if isinstance(res, str) and res else None
        except HTTPError:
            return None

    # 1. Wait for server. urlopen treats HTTP 4xx/5xx as HTTPError; any other
    # network error is URLError (e.g. DNS, connection refused).
    waited = 0
    ready = False
    while waited < wait_max_seconds:
        try:
            _api("/version", json_resp=False)
            ready = True
            break
        except (HTTPError, URLError, TimeoutError):
            time.sleep(wait_interval_seconds)
            waited += wait_interval_seconds
    if not ready:
        raise RuntimeError(
            f"windmill at {base} did not become ready in {wait_max_seconds}s"
        )

    # 2/3. Login. Prefer the desired password (idempotent re-run case) and
    # fall back to the default (first install). If we got in with the
    # default, rotate immediately so subsequent runs find a non-default
    # password and the workspace's actual superadmin password stays
    # consistent with whatever the caller stored alongside the token.
    session = _try_login(admin_email, desired_password)
    if not session:
        session = _try_login(admin_email, default_password)
        if not session:
            raise RuntimeError(
                f"cannot log in as {admin_email} with desired OR default password"
            )
        if desired_password != default_password:
            _api(
                "/users/setpassword",
                method="POST",
                body={"password": desired_password},
                token=session,
                json_resp=False,
            )
            session = _try_login(admin_email, desired_password)
            if not session:
                raise RuntimeError(
                    "password rotation succeeded but re-login failed"
                )

    # 4. Workspace (idempotent: swallow any failure — token mint below
    # surfaces a genuinely missing workspace via /api/w/<ws>/* 401s).
    # Windmill returns HTTP 400 with body "Workspace already exists" on a
    # second create; we don't try to parse that.
    try:
        _api(
            "/workspaces/create",
            method="POST",
            body={"id": workspace, "name": workspace},
            token=session,
            json_resp=False,
        )
    except Exception:
        pass

    # 5. Mint a non-expiring API token.
    token = _api(
        "/users/tokens/create",
        method="POST",
        body={"label": token_label, "expiration": None},
        token=session,
        json_resp=False,
    )
    if not token or not isinstance(token, str):
        raise RuntimeError(f"token mint failed: {token!r}")

    return token


def _bootstrap_from_env() -> None:
    """CLI entrypoint.

    Reads config from env vars and prints the minted token as the LAST
    line of stdout so callers can capture it with `tail -1`. Progress and
    informational output goes to stderr.

    Required env vars:
        WINDMILL_URL                 e.g. "http://windmill-server"
        WINDMILL_WORKSPACE           e.g. "goat"
        WINDMILL_ADMIN_EMAIL         e.g. "admin@windmill.dev"
        WINDMILL_ADMIN_PASSWORD      desired password (the password the
                                     caller will use afterwards)

    Optional env vars (with defaults):
        WINDMILL_DEFAULT_PASSWORD    default: "changeme"  (windmill's
                                                          first-boot pwd)
        WINDMILL_TOKEN_LABEL         default: "goatlib-bootstrap"
    """
    try:
        url = os.environ["WINDMILL_URL"]
        workspace = os.environ["WINDMILL_WORKSPACE"]
        admin_email = os.environ["WINDMILL_ADMIN_EMAIL"]
        desired_password = os.environ["WINDMILL_ADMIN_PASSWORD"]
    except KeyError as missing:
        print(
            f"ERROR: required env var missing: {missing.args[0]}",
            file=sys.stderr,
        )
        sys.exit(1)

    default_password = os.environ.get("WINDMILL_DEFAULT_PASSWORD", "changeme")
    token_label = os.environ.get("WINDMILL_TOKEN_LABEL", "goatlib-bootstrap")

    print(f"Bootstrapping windmill workspace at {url}", file=sys.stderr)
    token = bootstrap(
        url=url,
        workspace=workspace,
        admin_email=admin_email,
        desired_password=desired_password,
        default_password=default_password,
        token_label=token_label,
    )
    print(
        f"Token minted (label={token_label}, length={len(token)} chars)",
        file=sys.stderr,
    )
    # Token last on stdout for easy capture by callers (e.g. `... | tail -1`).
    print(token)


if __name__ == "__main__":
    _bootstrap_from_env()
