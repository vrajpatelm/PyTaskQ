"""
pytaskq.net — safe outbound HTTP helpers for task handlers
===========================================================

Task handlers often fetch user-supplied URLs. Without a guard a caller could make
workers request internal services (localhost, 169.254.169.254 cloud metadata,
RFC1918 ranges, ...). `assert_safe_url` resolves the hostname and rejects any URL
whose DNS points at a non-public address; `SAFE_OPENER` re-validates every redirect
hop so http://public.host/ can't bounce a worker behind the firewall.

Use it in your own tasks:

    from pytaskq.net import assert_safe_url, SAFE_OPENER
    assert_safe_url(url)
    with SAFE_OPENER.open(url, timeout=10) as resp: ...
"""

import ipaddress
import socket
import urllib.parse
import urllib.request


class UnsafeURL(Exception):
    """Raised when a user-supplied URL targets a non-public address."""


def assert_safe_url(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise UnsafeURL(f"Only http/https URLs are allowed, got: {parsed.scheme!r}")
    host = parsed.hostname
    if not host:
        raise UnsafeURL("URL has no hostname")
    if host in ("localhost",) or host.endswith((".localhost", ".local", ".internal")):
        raise UnsafeURL(f"Access to internal host {host!r} is not allowed")
    try:
        addr_infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        raise UnsafeURL(f"Cannot resolve host: {host}")
    for info in addr_infos:
        ip = ipaddress.ip_address(info[4][0])
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local      # 169.254.0.0/16 — cloud metadata lives here
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            raise UnsafeURL(f"Access to non-public address {ip} is not allowed")


class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Re-validates every redirect target before following it."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        assert_safe_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


SAFE_OPENER = urllib.request.build_opener(SafeRedirectHandler())

# Backwards-compatible private aliases
_assert_safe_url = assert_safe_url
_SafeRedirectHandler = SafeRedirectHandler
_SAFE_OPENER = SAFE_OPENER
