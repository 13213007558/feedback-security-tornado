"""Drives both apps through the scenarios and prints, for each one, the
observed callback order and the staging-write count.

This sandbox forbids binding TCP/Unix listeners, so instead of a port we
hand the server end of a socketpair() directly to
tornado.http1connection.HTTP1ServerConnection -- the exact class
TCPServer uses for accepted connections (see HTTP1ServerConnection.
start_serving), so the framework code path under test is identical to a
real socket.

Run:  .venv/bin/python streaming_auth_demo/run_scenarios.py
"""

import asyncio
import logging
import socket

import tornado.iostream
from tornado.http1connection import HTTP1ServerConnection

import common
import repro_app
import fixed_app

logging.getLogger("tornado.access").setLevel(logging.CRITICAL)
logging.getLogger("tornado.application").setLevel(logging.CRITICAL)

CHUNK1 = b"A" * 32
CHUNK2 = b"B" * 32
SMUGGLED = b"GET /smuggled HTTP/1.1\r\nHost: x\r\n\r\n"  # looks like a new request

RESULTS = []


def serve(app, server_sock):
    stream = tornado.iostream.IOStream(server_sock)
    conn = HTTP1ServerConnection(stream)
    conn.start_serving(app)
    return conn


class Client:
    def __init__(self, sock):
        self.sock = sock
        self.sock.setblocking(False)
        self.loop = asyncio.get_running_loop()
        self.buf = b""

    async def send(self, data):
        try:
            await self.loop.sock_sendall(self.sock, data)
            return True
        except (BrokenPipeError, ConnectionResetError):
            return False

    async def read_response(self, timeout=5.0):
        """Returns HTTP status int, or 'TIMEOUT' / 'EOF'."""
        while b"\r\n\r\n" not in self.buf:
            try:
                chunk = await asyncio.wait_for(self.loop.sock_recv(self.sock, 65536), timeout)
            except (asyncio.TimeoutError, ConnectionResetError):
                return "TIMEOUT"
            if not chunk:
                return "EOF"
            self.buf += chunk
        head, rest = self.buf.split(b"\r\n\r\n", 1)
        lines = head.decode("latin1").split("\r\n")
        status = int(lines[0].split()[1])
        headers = {}
        for line in lines[1:]:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
        body = rest
        while len(body) < int(headers.get("content-length", 0)):
            try:
                chunk = await asyncio.wait_for(self.loop.sock_recv(self.sock, 65536), timeout)
            except (asyncio.TimeoutError, ConnectionResetError):
                break
            if not chunk:
                break
            body += chunk
        self.buf = b""
        return status

    async def read_until_eof_or_timeout(self, timeout=3.0):
        """Returns (bytes_seen, got_eof)."""
        data = b""
        while True:
            try:
                chunk = await asyncio.wait_for(self.loop.sock_recv(self.sock, 65536), timeout)
            except (asyncio.TimeoutError, ConnectionResetError):
                return data, False
            if chunk == b"":
                return data, True
            data += chunk

    def close(self):
        self.sock.close()


def new_conn(app):
    server_sock, client_sock = socket.socketpair()
    serve(app, server_sock)
    return Client(client_sock)


def request_headers(mode, content_length):
    return (
        "POST /upload HTTP/1.1\r\nHost: test\r\nX-Auth-Mode: %s\r\n"
        "Content-Length: %d\r\n\r\n" % (mode, content_length)
    ).encode()


def record(app_name, scenario, status, extra=""):
    RESULTS.append((app_name, scenario, status, common.STAGING_WRITES,
                    common.STAGING_BYTES, list(common.EVENTS), extra))


async def scenario_denied(app, app_name):
    """Not logged in.  A correct server rejects before the body is
    complete; leftover bytes must not become another request."""
    common.reset()
    client = new_conn(app)
    # Declare a body longer than what we send up front, so an early
    # rejection would arrive while body bytes are still unsent.
    await client.send(request_headers("deny", len(CHUNK1) + len(CHUNK2) + len(SMUGGLED)))
    await client.send(CHUNK1)
    await asyncio.sleep(0.2)
    await client.send(CHUNK2)
    early = await client.read_response(timeout=2.0)
    if early == "TIMEOUT":
        # Server did not reject early: it is still waiting for the rest
        # of the body.  Complete the declared body to get the verdict.
        await client.send(SMUGGLED)
        status = await client.read_response()
        extra = ("no early rejection (client had to finish the body first); "
                 "early read: %s" % early)
    else:
        status = early
        # Response already sent while body bytes remain.  The leftover
        # bytes are shaped like a pipelined request; they must be
        # discarded, not parsed.
        await client.send(SMUGGLED)
        await client.send(b"GET /smuggled2 HTTP/1.1\r\nHost: x\r\n\r\n")
        trailing, got_eof = await client.read_until_eof_or_timeout()
        await asyncio.sleep(0.2)
        smuggled = [e for e in common.EVENTS if "smuggled" in str(e)]
        extra = ("early rejection; post-reject bytes discarded: eof=%s, "
                 "extra_response=%r, smuggled_events=%r" % (got_eof, trailing, smuggled))
    await asyncio.sleep(0.2)
    record(app_name, "denied (not logged in)", status, extra)
    client.close()


async def scenario_error(app, app_name):
    """Identity check raises."""
    common.reset()
    client = new_conn(app)
    await client.send(request_headers("error", len(CHUNK1) + len(CHUNK2)))
    await client.send(CHUNK1)
    await asyncio.sleep(0.2)
    await client.send(CHUNK2)
    status = await client.read_response()
    await asyncio.sleep(0.2)
    record(app_name, "auth check raises", status)
    client.close()


async def scenario_block(app, app_name, outcome):
    """Client keeps sending body while the check is still parked."""
    common.reset()
    client = new_conn(app)
    await client.send(request_headers("block-then-" + outcome, len(CHUNK1) + len(CHUNK2)))
    await asyncio.sleep(0.3)
    await client.send(CHUNK1)
    await asyncio.sleep(0.3)
    await client.send(CHUNK2)
    await asyncio.sleep(0.4)
    writes_while_pending = common.STAGING_WRITES
    common.release_auth()
    status = await client.read_response()
    await asyncio.sleep(0.2)
    record(
        app_name,
        "body sent while check pending (release=%s)" % outcome,
        status,
        "staging writes while check pending: %d" % writes_while_pending,
    )
    client.close()


async def scenario_ok(app, app_name):
    """Normal authenticated upload."""
    common.reset()
    client = new_conn(app)
    await client.send(request_headers("ok", len(CHUNK1) + len(CHUNK2)))
    await client.send(CHUNK1)
    await asyncio.sleep(0.2)
    await client.send(CHUNK2)
    status = await client.read_response()
    await asyncio.sleep(0.2)
    record(app_name, "normal authenticated upload", status)
    client.close()


async def run_app(app_mod, app_name):
    app = app_mod.make_app()
    await scenario_denied(app, app_name)
    await scenario_error(app, app_name)
    await scenario_block(app, app_name, "deny")
    await scenario_block(app, app_name, "ok")
    await scenario_ok(app, app_name)


def report():
    for app_name, scenario, status, writes, nbytes, events, extra in RESULTS:
        print("\n[%s] %s" % (app_name, scenario))
        print("  response: %s   staging writes: %d (%d bytes)" % (status, writes, nbytes))
        if extra:
            print("  %s" % extra)
        for i, (event, detail) in enumerate(events, 1):
            print("  %2d. %-22s %s" % (i, event, detail))


def check():
    failures = []

    def find(app_name, scenario_sub):
        for r in RESULTS:
            if r[0] == app_name and scenario_sub in r[1]:
                return r
        return None

    # Hard requirement: fixed app, unauthorized scenarios -> zero staging writes.
    for scen in ("denied", "raises", "release=deny"):
        r = find("fixed", scen)
        if r is None or r[3] != 0:
            failures.append("fixed/%s: staging writes = %s (must be 0)" % (scen, r and r[3]))
    # Fixed app: data_received must never appear before auth:result.
    for r in RESULTS:
        if r[0] != "fixed":
            continue
        order = [e for e, _ in r[5]]
        if "data_received" in order and order.index("data_received") < order.index("auth:result"):
            failures.append("fixed/%s: data_received before auth:result" % r[1])
    # Fixed app: rejection arrives while body bytes are still unsent, and
    # the leftover bytes are discarded (connection closed, nothing parsed).
    r = find("fixed", "denied")
    if r is None or r[2] != 403 or "eof=True" not in r[6] or "extra_response=b''" not in r[6] \
            or "smuggled_events=[]" not in r[6]:
        failures.append("fixed/denied: bad early-reject/discard behavior: %s" % (r and (r[2], r[6])))
    # Repro app must show the leak (proves the repro is valid) and must
    # be unable to reject before the body is complete.
    r = find("repro", "denied")
    if r is None or r[3] == 0 or "no early rejection" not in r[6]:
        failures.append("repro/denied: expected staging leak and no early rejection: %s" % (r and (r[3], r[6])))
    r = find("repro", "raises")
    if r is None or r[3] == 0:
        failures.append("repro/error: expected staging writes > 0 to demonstrate the leak")

    print("\n" + "=" * 70)
    if failures:
        print("CHECK FAILURES:")
        for f in failures:
            print("  - " + f)
        return 1
    print("ALL CHECKS PASSED")
    return 0


async def main():
    await run_app(repro_app, "repro")
    await run_app(fixed_app, "fixed")


if __name__ == "__main__":
    asyncio.run(main())
    report()
    raise SystemExit(check())
