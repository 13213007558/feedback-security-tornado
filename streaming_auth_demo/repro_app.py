"""REPRO: the pattern from the bug report.

@stream_request_body + @authenticated on post().

The identity check only runs when post() is invoked -- i.e. AFTER the
entire request body has already been streamed through data_received()
into the staging area.  @authenticated also requires a *synchronous*
get_current_user (see RequestHandler.current_user docs), so with this
pattern there is no hook that could run the check before the body.

Run:  python3 repro_app.py     # serves on http://127.0.0.1:8891/upload
"""

import tornado.ioloop
import tornado.web

import common
from common import BaseUploadHandler


@tornado.web.stream_request_body
class UploadHandler(BaseUploadHandler):
    def get_current_user(self):
        # Synchronous, as required by @authenticated.  A "block-then-*"
        # mode cannot actually pause here: the check only exists once
        # post() runs, which is already after the whole body.
        mode = self.request.headers.get("X-Auth-Mode", "ok")
        _, mode = common.resolve_mode(mode)
        common.log("get_current_user", mode)
        if mode == "error":
            raise RuntimeError("auth backend exploded")
        return None if mode == "deny" else "alice"

    @tornado.web.authenticated
    async def post(self):
        common.log("post", "user=%r" % self.current_user)
        self.write({"stored_bytes": common.STAGING_BYTES})


def make_app():
    return tornado.web.Application([(r"/upload", UploadHandler)])


if __name__ == "__main__":
    app = make_app()
    app.listen(8891)
    print("repro app on http://127.0.0.1:8891/upload")
    tornado.ioloop.IOLoop.current().start()
