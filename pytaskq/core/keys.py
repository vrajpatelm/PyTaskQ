"""
pytaskq.core.keys — every Redis key name goes through here
==========================================================

`REDIS_KEY_PREFIX` puts all of PyTaskQ's keys (and its pub/sub channel) under
one prefix, so several PyTaskQ installations, or PyTaskQ and another app, can
share one Redis database without seeing each other's queues, jobs or accounts.

The prefix is wrapped in braces — `REDIS_KEY_PREFIX=shop` gives `{shop}:queue:high`,
`{shop}:task:…` — which makes it a Redis Cluster "hash tag": every key lands in
the same cluster slot, as the multi-key Lua scripts require. (PyTaskQ's client
doesn't talk to Redis Cluster yet, but the key layout no longer prevents it.)

Without a prefix (the default) the key names are exactly the ones earlier
versions used, so upgrading moves nothing. Changing the prefix of an existing
installation starts from an empty one: the old keys are left where they are.

The prefix is read once at startup; every process of one installation (API,
workers, your app) must use the same value.
"""

from __future__ import annotations

import re

from pytaskq.core.config import settings

PLACEHOLDER = "__PYTASKQ__"           # stands for the prefix inside Lua scripts
_VALID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,49}$")


def _compute(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    if not _VALID.match(raw):
        raise SystemExit(f"REDIS_KEY_PREFIX={raw!r} isn't valid: use up to 50 letters, digits, '.', '_' or '-'.")
    return "{" + raw + "}:"


PREFIX = _compute(settings.REDIS_KEY_PREFIX)


def k(name: str) -> str:
    """A Redis key (or channel) name with this installation's prefix."""
    return PREFIX + name


def strip(key: str) -> str:
    """The key name without the prefix (for keys found with SCAN)."""
    return key[len(PREFIX):] if PREFIX and key.startswith(PREFIX) else key


def lua(script: str) -> str:
    """A Lua script with the prefix filled in where it names keys or the channel itself."""
    return script.replace(PLACEHOLDER, PREFIX)


def fill_scripts(namespace: dict) -> None:
    """Fill the prefix into every Lua script (string constant) of a module, once, at import."""
    for name, value in list(namespace.items()):
        if isinstance(value, str) and PLACEHOLDER in value:
            namespace[name] = lua(value)
