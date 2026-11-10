"""HTTPServer 连接限制配置：小认证请求与大流式审计上传共存。

设计要点（依据本仓库 tornado 6.7.dev1 源码核实）：

* ``idle_connection_timeout`` 在 HTTPServer.initialize 里被映射为
  ``HTTP1ConnectionParameters.header_timeout``（httpserver.py:189），
  它只管“等一个请求的完整头部”这一段，默认 3600s；传 0 会被
  ``or 3600`` 当成假值而退回默认，因此必须显式给正数。
* ``body_timeout`` 是整个正文读取的一次性总截止时间，不是块间空闲超时
  （http1connection.py:309-321），所以慢正文防御要靠“总时长 + 总量”两条，
  上传路由需要同时放大额度与总时长。
* ``max_body_size`` 默认取 IOStream.max_buffer_size（100MiB）。显式给一个
  小的服务级默认，再由上传 handler 在 prepare() 里按请求放大。
* HTTP1ServerConnection 每个请求都 new 一个 HTTP1Connection
  （http1connection.py:951），其构造函数从 params 复制初始限制
  （http1connection.py:160-165），所以 set_max_body_size/set_body_timeout
  的修改只活在本次请求对象上，下一个请求天然回到服务级默认，
  无需手工还原。
"""

from __future__ import annotations

import typing

import tornado.httpserver
import tornado.web
from tornado.http1connection import HTTP1Connection

# 服务级保守默认：认证等小请求用这套就够。
IDLE_CONNECTION_TIMEOUT = 5.0        # 等待完整请求头 / keep-alive 空闲上限（秒）
BODY_TIMEOUT = 10.0                  # 正文读取总时长上限（秒）
MAX_HEADER_SIZE = 8 * 1024           # 请求头 8KiB（默认 64KiB）
MAX_BODY_SIZE = 64 * 1024            # 普通请求正文 64KiB（默认退化为 100MiB）
MAX_BUFFER_SIZE = 4 * 1024 * 1024    # IOStream 底层读缓冲 4MiB

# 上传路由的放大额度，仅作用于命中 /audit/upload 的那一次请求。
UPLOAD_MAX_BODY_SIZE = 2 * 1024 * 1024   # 2MiB 流式审计上传
UPLOAD_BODY_TIMEOUT = 120.0              # 大正文允许的总读取时长（秒）


def build_server(application: tornado.web.Application) -> tornado.httpserver.HTTPServer:
    """用保守的服务级默认构造 HTTPServer。"""
    return tornado.httpserver.HTTPServer(
        application,
        idle_connection_timeout=IDLE_CONNECTION_TIMEOUT,
        body_timeout=BODY_TIMEOUT,
        max_header_size=MAX_HEADER_SIZE,
        max_body_size=MAX_BODY_SIZE,
        max_buffer_size=MAX_BUFFER_SIZE,
    )


class AuthHandler(tornado.web.RequestHandler):
    """小型认证请求：不做任何放宽，完全沿用服务级小额度。"""

    def post(self) -> None:
        if len(self.request.body) > MAX_BODY_SIZE:
            # 理论上到不了：超限会在协议层直接 400，这里只是显式表达意图。
            raise tornado.web.HTTPError(400, "body too large")
        self.write({"ok": True, "bytes": len(self.request.body)})


@tornado.web.stream_request_body
class AuditUploadHandler(tornado.web.RequestHandler):
    """流式审计上传：在正文读取开始前放大本次请求的额度。

    必须使用 stream_request_body。缓冲式 handler 的 handler 对象在整个正文
    读完后的 finish() 阶段才创建（web.py _HandlerDelegate.finish），那时再调
    set_max_body_size 已经晚了；流式模式下 prepare() 先于 data_received 执行，
    调整能赶上 _read_body 的 Content-Length 预检查与后续累计计数。
    """

    def prepare(self) -> None:
        connection = typing.cast(HTTP1Connection, self.request.connection)
        connection.set_max_body_size(UPLOAD_MAX_BODY_SIZE)
        connection.set_body_timeout(UPLOAD_BODY_TIMEOUT)
        self._bytes_read = 0

    def data_received(self, chunk: bytes) -> None:
        self._bytes_read += len(chunk)

    def post(self) -> None:
        self.write({"ok": True, "bytes": self._bytes_read})


def build_app() -> tornado.web.Application:
    return tornado.web.Application(
        [
            (r"/auth", AuthHandler),
            (r"/audit/upload", AuditUploadHandler),
        ]
    )
