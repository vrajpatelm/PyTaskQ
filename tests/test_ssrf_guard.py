"""
test_ssrf_guard.py — Tests for the SSRF protection in task handlers
====================================================================

WHAT WE TEST HERE:
    _assert_safe_url() must reject user-supplied URLs that would make the
    worker talk to internal infrastructure:
      - cloud metadata endpoint (169.254.169.254 — link-local)
      - loopback (127.0.0.1, localhost)
      - RFC1918 private ranges (10.x, 192.168.x, 172.16-31.x)
      - non-http schemes (file://, ftp://, gopher://)
      - unresolvable hostnames

    And it must ALLOW genuine public URLs (validated via DNS only —
    no network request is made in these tests).

ARCHITECTURE TESTED:
    task_registery._assert_safe_url / _SafeRedirectHandler
"""

import importlib.util
import os

import pytest


def _load_real_task_registery():
    """
    conftest.py injects a MagicMock under sys.modules["task_registery"] to
    shield other suites from yagmail/SMTP. For THESE tests we need the real
    module, so we load the file directly from src/ by path.
    """
    path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "src", "task_registery.py")
    )
    spec = importlib.util.spec_from_file_location("real_task_registery", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_real = _load_real_task_registery()
UnsafeURL = _real.UnsafeURL
_assert_safe_url = _real._assert_safe_url


# ── Blocked: internal / dangerous targets ─────────────────────────────────────

@pytest.mark.parametrize("url", [
    "http://169.254.169.254/latest/meta-data/",   # AWS/GCP/Azure metadata
    "http://metadata.google.internal/",           # GCP metadata hostname
    "http://127.0.0.1:8000/admin",                # loopback
    "http://localhost:6379/",                     # Redis on localhost
    "http://10.0.0.5/internal-service",           # RFC1918
    "http://192.168.1.1/router",                  # RFC1918
    "http://172.16.0.9/",                         # RFC1918
    "http://0.0.0.0/",                            # unspecified
    "file:///etc/passwd",                         # scheme abuse
    "ftp://public.example.com/file",              # non-http scheme
    "gopher://127.0.0.1:6379/_FLUSHALL",          # gopher → Redis replay attacks
    "http://this-host-does-not-exist-xyz123/",    # DNS failure
])
def test_ssrf_guard_blocks_internal_urls(url):
    with pytest.raises(UnsafeURL):
        _assert_safe_url(url)


# ── Allowed: genuine public URLs (DNS-only check, no request) ─────────────────

@pytest.mark.parametrize("url", [
    "https://example.com/image.jpg",
    "https://www.python.org/robots.txt",
    "http://api.github.com/zen",   # http (not just https) must still work
])
def test_ssrf_guard_allows_public_urls(url):
    _assert_safe_url(url)  # must not raise
