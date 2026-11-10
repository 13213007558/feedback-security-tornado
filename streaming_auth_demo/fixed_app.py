"""FIXED: same upload endpoint, identity check moved into prepare().

Tornado's HTTP/1 connection awaits delegate.headers_received() -- which
for a streaming handler resolves only when prepare() has completed
(RequestHandler._prepared_future) -- before it reads a single body byte
off the socket.  Doing the async check in prepare() therefore means an
unauthorized request never reaches data_received(): zero staging writes,
without buffering the body and without re-checking inside post().

Run:  python3 fixed_app.py     # serves on http://127.0.0.1:8892/upload
"""

import tornado.ioloop
import tornado.web

import common
from common import BaseUploadHandler


@tornado.web.stream_request_body
class UploadHandler(BaseUploadHandler):
    async def prepare(self):
        BaseUploadHandler.prepare(self)  # logs "prepare"
        # Async identity check, in the one place the framework gates
        # body delivery on.  Raising here => body is never delivered.
        self.current_user = await self.check_auth()
        if self.current_user is None:
            raise tornado.web.HTTPError(403)

    @tornado.web.authenticated
    async def post(self):
        # Unchanged: passes because prepare() already set current_user.
        common.log("post", "user=%r" % self.current_user)
        self.write({"stored_bytes": common.STAGING_BYTES})


def make_app():
    return tornado.web.Application([(r"/upload", UploadHandler)])


if __name__ == "__main__":
    app = make_app()
    app.listen(8892)
    print("fixed app on http://127.0.0.1:8892/upload")
    tornado.ioloop.IOLoop.current().start()
