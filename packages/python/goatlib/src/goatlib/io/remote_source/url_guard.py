"""Refuse user-supplied URLs that would make the server fetch from inside.

A WFS import runs in the worker, so whatever address a user types in is
fetched from our network, not theirs: without a check, `http://169.254.169.254/`
or a cluster-internal service name reads back through the import. Only http(s)
addresses whose every resolved IP is globally routable pass.

Residuals, accepted for now: the name is resolved again when GDAL connects
(DNS rebinding), and GDAL follows HTTP redirects without asking us.
"""

from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlparse


class UnsafeUrlError(ValueError):
    """The URL is not an http(s) address that resolves to public IPs only."""


def assert_public_http_url(url: str) -> None:
    """Raise `UnsafeUrlError` unless `url` is http(s) and resolves to public IPs."""
    parsed = urlparse(url.strip())
    if parsed.scheme not in ("http", "https"):
        raise UnsafeUrlError("Only http and https service addresses can be used")
    host = parsed.hostname
    if not host:
        raise UnsafeUrlError("The service address has no host")

    try:
        infos = socket.getaddrinfo(host, parsed.port, proto=socket.IPPROTO_TCP)
    except (socket.gaierror, UnicodeError) as exc:
        raise UnsafeUrlError(f"Could not resolve {host}") from exc

    for info in infos:
        # Strip an IPv6 zone id (`fe80::1%eth0`) before parsing.
        address = ipaddress.ip_address(str(info[4][0]).split("%", 1)[0])
        if not address.is_global:
            raise UnsafeUrlError(f"{host} resolves to a non-public address")
