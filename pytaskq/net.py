"""
pytaskq.net — safe outbound HTTP helpers for task handlers
===========================================================

Task handlers often fetch user-supplied URLs. Without a guard a caller could make
workers request internal services (localhost, 169.254.169.254 cloud metadata,
RFC1918 ranges, ...). `assert_safe_url` resolves the hostname and rejects any URL
whose DNS points at a non-public address (loopback, private, link-local, CGNAT…); `SAFE_OPENER` re-validates every redirect
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
        ip = ipaddress.ip_address(info[4][0].split("%", 1)[0])   # drop an IPv6 zone id
        # ::ffff:127.0.0.1 is 127.0.0.1 written as IPv6: judge the IPv4 address
        # (older Pythons call the mapped form "global").
        if ip.version == 6 and ip.ipv4_mapped is not None:
            ip = ip.ipv4_mapped
        # Only addresses that are reachable on the public internet are allowed.
        # `is_global` also rules out ranges the older checks missed, such as
        # carrier-grade NAT (100.64.0.0/10) used inside many clouds and VPNs.
        if not ip.is_global or ip.is_multicast:
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
