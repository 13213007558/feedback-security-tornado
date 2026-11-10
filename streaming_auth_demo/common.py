"""Shared plumbing for the streaming-auth demo.

The "staging area" simulates what a real upload handler does inside
data_received(): spill every chunk to a temp file before the upload is
complete.  STAGING_WRITES counts how many times business data was
written there -- for unauthorized requests it must stay 0.
"""

import asyncio
import os
import tempfile

import tornado.web

EVENTS = []          # ordered callback log: (event, detail)
STAGING_WRITES = 0   # number of chunk writes into the staging area
STAGING_BYTES = 0
STAGING_DIR = os.path.join(tempfile.gettempdir(), "tornado-stream-auth-staging")

# Pausable auth gate: while it is cleared, check_auth() is parked.
AUTH_GATE = None        # asyncio.Event, created per scenario


def reset():
    global STAGING_WRITES, STAGING_BYTES, AUTH_GATE
    del EVENTS[:]
    STAGING_WRITES = 0
    STAGING_BYTES = 0
    AUTH_GATE = asyncio.Event()
    os.makedirs(STAGING_DIR, exist_ok=True)


def log(event, detail=""):
    EVENTS.append((event, detail))


def release_auth():
    AUTH_GATE.set()


def resolve_mode(mode):
    """X-Auth-Mode: ok | deny | error | block-then-ok | block-then-deny.

    Returns (wait_first, effective_mode).
    """
    if mode.startswith("block-then-"):
        return True, mode.rsplit("-", 1)[1]
    return False, mode


class BaseUploadHandler(tornado.web.RequestHandler):
    """Pausable identity check + staging data_received, shared by both apps."""

    def initialize(self):
        os.makedirs(STAGING_DIR, exist_ok=True)
        self.staging_path = os.path.join(STAGING_DIR, "upload-%d.part" % id(self))

    def prepare(self):
        log("prepare")

    async def check_auth(self):
        """Pausable identity check ("block-then-*" parks on AUTH_GATE,
        simulating a slow identity provider)."""
        mode = self.request.headers.get("X-Auth-Mode", "ok")
        log("auth:start", mode)
        wait, mode = resolve_mode(mode)
        if wait:
            log("auth:waiting", "check parked, client may keep sending body")
            await AUTH_GATE.wait()
            log("auth:resumed", mode)
        if mode == "error":
            raise RuntimeError("auth backend exploded")
        user = None if mode == "deny" else "alice"
        log("auth:result", repr(user))
        return user

    def data_received(self, chunk):
        global STAGING_WRITES, STAGING_BYTES
        STAGING_WRITES += 1
        STAGING_BYTES += len(chunk)
        with open(self.staging_path, "ab") as f:
            f.write(chunk)
        log("data_received", "%d bytes -> staging (write #%d)" % (len(chunk), STAGING_WRITES))

    def on_connection_close(self):
        log("on_connection_close")
