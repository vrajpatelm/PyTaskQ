"""
pytaskq.core.keys — every Redis key name goes through here
==========================================================

`REDIS_KEY_PREFIX` puts all keys (and the pub/sub channel) under one prefix, so several
installations can share one Redis. It's wrapped in braces (`{shop}:queue:high`), a Redis
Cluster hash tag: every key lands in one slot, as the multi-key Lua scripts require.

Without a prefix the key names are unchanged. Changing the prefix starts from an empty
installation. Every process of one installation must use the same value.
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


def layout(raw_prefix: str = "") -> dict:
    """A few key names as another installation would write them (REDIS_KEY_PREFIX=raw_prefix, "" = none):
    to notice data written under a different prefix."""
    p = _compute(raw_prefix)
    return {"install": p + "pytaskq:install", "queues": [p + "queue:high", p + "queue:default", p + "queue:low"],
            "install_pattern": "{*}:pytaskq:install"}
