"""可控 IO 验证脚本：不绑端口、不做压测，逐项核实连接限制的生效边界。

运行：.venv/bin/python -m connection_limits.verify_limits

做法：用一个内存版 FakeStream 替换真实 socket，测试代码决定“什么时候喂几个
字节”，Tornado 侧仍跑真实的 HTTP1ServerConnection / web.Application 代码路径，
所有超时走真实 IOLoop 的短时间定时器（0.05~0.3s），单次脚本总耗时数秒。
"""

from __future__ import annotations

import asyncio
import re
import socket
import unittest
from typing import Any

import tornado.web
from tornado.concurrent import Future
from tornado.http1connection import (
    HTTP1ConnectionParameters,
    HTTP1ServerConnection,
)
from tornado.httputil import (
    HTTPHeaders,
    HTTPMessageDelegate,
    HTTPServerConnectionDelegate,
    ResponseStartLine,
)
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError, UnsatisfiableReadError

from connection_limits.server_config import (
    MAX_BODY_SIZE,
    MAX_HEADER_SIZE,
    UPLOAD_BODY_TIMEOUT,
    UPLOAD_MAX_BODY_SIZE,
    AuthHandler,
    build_app,
)

# 测试里用短时间替代生产值，保持事件真实但不拖慢用例。
T_HEADER = 0.10
T_BODY = 0.15


class FakeStream:
    """内存 IOStream：只实现 HTTP1Connection 会用到的接口。"""

    def __init__(self, io_loop: IOLoop) -> None:
        self.io_loop = io_loop
        self._read_buffer = bytearray()
        self._pending: tuple[str, Any, Future] | None = None
        self._closed = False
        self._close_callbacks: list[Any] = []
        self.write_buffer = bytearray()
        self.max_buffer_size = 4 * 1024 * 1024
        # _HTTPRequestContext 会读 socket.family。
        self.socket = type("S", (), {"family": socket.AF_INET})()

    # ---- 连接状态 ----
    def closed(self) -> bool:
        return self._closed

    def close(self, exc_info: Any = None) -> None:
        if self._closed:
            return
        self._closed = True
        pending = self._pending
        self._pending = None
        if pending is not None:
            kind, _arg, future = pending
            if not future.done():
                future.set_exception(StreamClosedError("stream closed"))
        callbacks = self._close_callbacks
        self._close_callbacks = []
        for callback in callbacks:
            self.io_loop.add_callback(callback)

    def set_close_callback(self, callback: Any) -> None:
        # IOStream 只保留一个回调；FakeStream 用列表支持测试侧额外挂等待者。
        if callback is None:
            self._close_callbacks = []
        else:
            self._close_callbacks.append(callback)

    def set_nodelay(self, value: bool) -> None:
        pass

    # ---- 写侧：进内存缓冲。Tornado 的 done 回调对已完成 future 是同步触发，
    # 因此这里立即在 future 上执行回调，而不是推迟到下一轮事件循环。 ----
    def write(self, data: bytes) -> Future:
        self.write_buffer += data
        future: Future = Future()
        future.set_result(None)
        return future

    # ---- 读侧 ----
    def read_bytes(self, num_bytes: int, partial: bool = False) -> Future:
        future: Future = Future()
        self._pending = ("bytes", (num_bytes, partial), future)
        self._pump()
        return future

    def read_until(self, delimiter: bytes, max_bytes: int | None = None) -> Future:
        future = Future()
        self._pending = ("until", (delimiter, max_bytes), future)
        self._pump()
        return future

    def read_until_regex(self, pattern: Any, max_bytes: int | None = None) -> Future:
        # IOStream 接受 bytes 正则并在内部编译，FakeStream 保持同样行为。
        compiled = re.compile(pattern) if isinstance(pattern, bytes) else pattern
        future = Future()
        self._pending = ("regex", (compiled, max_bytes), future)
        self._pump()
        return future

    def feed(self, data: bytes) -> None:
        """测试侧注入对端发来的字节，并尝试满足挂起的读。"""
        if self._closed:
            raise ConnectionError("feed after close")
        self._read_buffer += data
        # 模拟 IOStream 的 socket 可读事件：数据到达后若正有挂起读，立即内联处理。
        if self._pending is not None:
            self._pump()

    def _pump(self) -> None:
        if self._pending is None or self._closed:
            return
        kind, arg, future = self._pending
        if kind == "bytes":
            num_bytes, _partial = arg
            if not self._read_buffer:
                return
            take = min(num_bytes, len(self._read_buffer))
            data = bytes(self._read_buffer[:take])
            del self._read_buffer[:take]
            self._pending = None
            future.set_result(data)
        elif kind in ("until", "regex"):
            if kind == "until":
                delimiter, max_bytes = arg
                idx = self._read_buffer.find(delimiter)
                if idx >= 0:
                    checked_size = idx + len(delimiter)
                    hit = checked_size
                else:
                    checked_size = len(self._read_buffer)
                    hit = None
            else:
                pattern, max_bytes = arg
                match = pattern.search(self._read_buffer)
                if match is not None:
                    checked_size = match.end()
                    hit = checked_size
                else:
                    checked_size = len(self._read_buffer)
                    hit = None
            # 对齐 IOStream._check_max_bytes：命中时用命中位置长度，
            # 未命中时用当前缓冲长度，二者都与 max_bytes 比较。
            if max_bytes is not None and checked_size > max_bytes:
                self._pending = None
                if not future.done():
                    future.set_exception(
                        UnsatisfiableReadError("delimiter not within max_bytes")
                    )
                self.close(exc_info=UnsatisfiableReadError("max_bytes exceeded"))
            elif hit is not None:
                data = bytes(self._read_buffer[:hit])
                del self._read_buffer[:hit]
                self._pending = None
                future.set_result(data)


class RecordingDelegate(HTTPMessageDelegate):
    """协议层用 delegate：记录回调，不挂任何应用逻辑。"""

    def __init__(self, connection: Any) -> None:
        self.connection = connection
        self.headers_seen: list[Any] = []
        self.chunks: list[bytes] = []
        self.finished = 0
        self.closed = 0

    def headers_received(self, start_line: Any, headers: Any) -> Any:
        self.headers_seen.append((start_line, headers))
        return None

    def data_received(self, chunk: bytes) -> Any:
        self.chunks.append(chunk)
        return None

    def finish(self) -> None:
        self.finished += 1
        # 真实应用一定会写响应；写出后连接才会回到下一轮头部读取。
        # 仅在连接仍存活时写，避免对已关闭连接重复操作。
        if not self.connection.stream.closed():
            self.connection.write_headers(
                ResponseStartLine("HTTP/1.1", 200, "OK"),
                HTTPHeaders({"Content-Length": "0"}),
            )
            self.connection.finish()

    def on_connection_close(self) -> None:
        self.closed += 1


class RecordingServerDelegate(HTTPServerConnectionDelegate):
    """HTTPServerConnectionDelegate 的最小实现。"""

    def __init__(self) -> None:
        self.message_delegates: list[RecordingDelegate] = []

    def start_request(self, server_conn: Any, request_conn: Any) -> Any:
        delegate = RecordingDelegate(request_conn)
        self.message_delegates.append(delegate)
        return delegate

    def on_close(self, server_conn: Any) -> None:
        pass


def params(**overrides: Any) -> HTTP1ConnectionParameters:
    base = dict(
        max_header_size=MAX_HEADER_SIZE,
        header_timeout=T_HEADER,
        max_body_size=MAX_BODY_SIZE,
        body_timeout=T_BODY,
    )
    base.update(overrides)
    return HTTP1ConnectionParameters(**base)


def make_server_connection(
    stream: FakeStream, cp: HTTP1ConnectionParameters
) -> tuple[HTTP1ServerConnection, RecordingServerDelegate]:
    delegate = RecordingServerDelegate()
    connection = HTTP1ServerConnection(stream, cp, context=type("Ctx", (), {})())
    connection.start_serving(delegate)
    return connection, delegate


class LimitBoundaryTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.io_loop = IOLoop.current()

    def _stream(self) -> FakeStream:
        return FakeStream(self.io_loop)

    async def _wait_closed(self, stream: FakeStream, timeout: float = 2.0) -> None:
        # FakeStream 的关闭回调经 add_callback 异步派发，注册时机可能与关闭竞态，
        # 这里用短轮询等待 closed 标志，避免依赖回调注册顺序。
        deadline = self.io_loop.time() + timeout
        while not stream.closed() and self.io_loop.time() < deadline:
            await asyncio.sleep(0.01)
        if not stream.closed():
            raise AssertionError("stream did not close within timeout")

    async def test_half_header_idle_hits_header_timeout(self) -> None:
        """只送半个请求头后静默：header_timeout 到期静默关闭，无 400 响应。"""
        stream = self._stream()
        _conn, delegate = make_server_connection(stream, params())
        stream.feed(b"POST /auth HTTP/1.1\r\nHost: x\r\nX-Junk: ")
        self.assertFalse(stream.closed())
        await self._wait_closed(stream)
        self.assertTrue(stream.closed())
        self.assertEqual(b"", stream.write_buffer)  # 超时路径不写任何响应
        self.assertEqual(delegate.message_delegates[0].headers_seen, [])
        # 头部未完整时 need_delegate_close 仍为 False，服务循环直接返回，
        # 不会再回调 delegate.on_connection_close（与 http1connection.py 行为一致）。
        self.assertEqual(delegate.message_delegates[0].closed, 0)

    async def test_header_oversize_closes_without_400(self) -> None:
        """请求头超过 max_header_size：UnsatisfiableReadError，连接关闭且无 400。"""
        stream = self._stream()
        _conn, delegate = make_server_connection(stream, params())
        big_header = b"X-Junk: " + b"a" * (MAX_HEADER_SIZE + 1024) + b"\r\n\r\n"
        stream.feed(b"POST /auth HTTP/1.1\r\nHost: 127.0.0.1\r\n" + big_header)
        await asyncio.sleep(0.05)
        self.assertTrue(stream.closed())
        self.assertEqual(b"", stream.write_buffer)  # 与 body 超限的 400 区分开
        self.assertEqual(delegate.message_delegates[0].headers_seen, [])

    async def test_slow_body_hits_body_timeout_total_deadline(self) -> None:
        """头部完整、正文慢速喂不饱：body_timeout 是总截止时间，到期关连接。"""
        stream = self._stream()
        _conn, delegate = make_server_connection(stream, params())
        stream.feed(
            b"POST /auth HTTP/1.1\r\nContent-Length: 10\r\n\r\n"
        )
        await asyncio.sleep(0.02)
        stream.feed(b"ab")  # 中途有数据也不重置总截止时间
        await self._wait_closed(stream)
        self.assertTrue(stream.closed())
        message = delegate.message_delegates[0]
        self.assertEqual(len(message.headers_seen), 1)  # 头部已交付应用层
        self.assertEqual(b"".join(message.chunks), b"ab")
        self.assertEqual(message.finished, 0)          # 正文没收完，没有 finish

    async def test_content_length_over_limit_returns_400_before_body(self) -> None:
        """Content-Length 超过 max_body_size：读正文前就 400 并关闭。"""
        stream = self._stream()
        _conn, delegate = make_server_connection(stream, params())
        too_big = MAX_BODY_SIZE + 1
        stream.feed(
            f"POST /auth HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {too_big}\r\n\r\n".encode()
        )
        await asyncio.sleep(0.05)
        self.assertTrue(stream.closed())
        self.assertIn(b"400 Bad Request", stream.write_buffer)
        self.assertEqual(delegate.message_delegates[0].finished, 0)

    async def test_chunked_body_over_limit_returns_400(self) -> None:
        """chunked 正文累计字节超过 max_body_size：累计计数超限后 400。"""
        stream = self._stream()
        _conn, _delegate = make_server_connection(stream, params())
        over = MAX_BODY_SIZE + 64
        stream.feed(
            b"POST /audit/upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\n\r\n"
        )
        stream.feed(f"{over:x}\r\n".encode() + b"z" * over + b"\r\n0\r\n\r\n")
        await asyncio.sleep(0.05)
        self.assertTrue(stream.closed())
        self.assertIn(b"400 Bad Request", stream.write_buffer)

    async def test_keepalive_idle_uses_header_timeout_per_request(self) -> None:
        """keep-alive 空闲：下一个请求的头部等待同样受 header_timeout 约束。"""
        stream = self._stream()
        _conn, delegate = make_server_connection(stream, params())
        stream.feed(b"GET /auth HTTP/1.1\r\nHost: x\r\n\r\n")
        await asyncio.sleep(0.05)
        first = delegate.message_delegates[0]
        self.assertEqual(first.finished, 1)
        self.assertFalse(stream.closed())  # 第一个请求正常结束，连接保留
        # 此后不再喂任何字节，模拟 keep-alive 空闲。
        await self._wait_closed(stream)
        self.assertTrue(stream.closed())
        # 第二轮循环会创建新 delegate 并开始等头部，但头部从未完整，
        # 因此只有第一个请求真正交付过 headers_received/finish。
        self.assertEqual(len(delegate.message_delegates[0].headers_seen), 1)
        second = delegate.message_delegates[-1]
        self.assertEqual(second.headers_seen, [])
        self.assertEqual(second.finished, 0)

    async def test_per_request_override_does_not_leak(self) -> None:
        """协议层：第一个请求放大额度，keep-alive 第二个请求回到服务级默认。"""
        stream = self._stream()
        cp = params()
        connection = HTTP1ServerConnection(stream, cp, context=type("Ctx", (), {})())

        class OverrideDelegate(RecordingDelegate):
            def headers_received(self, start_line: Any, headers: Any) -> Any:
                self.connection.set_max_body_size(UPLOAD_MAX_BODY_SIZE)
                self.connection.set_body_timeout(UPLOAD_BODY_TIMEOUT)
                return super().headers_received(start_line, headers)

        class ServerDelegate(HTTPServerConnectionDelegate):
            def __init__(self) -> None:
                self.delegates: list[RecordingDelegate] = []

            def start_request(self, server_conn: Any, request_conn: Any) -> Any:
                # 第一个请求放大，第二个请求不调整，验证连接对象每请求重建。
                if len(self.delegates) == 0:
                    delegate: RecordingDelegate = OverrideDelegate(request_conn)
                else:
                    delegate = RecordingDelegate(request_conn)
                self.delegates.append(delegate)
                return delegate

            def on_close(self, server_conn: Any) -> None:
                pass

        server_delegate = ServerDelegate()
        connection.start_serving(server_delegate)

        big = UPLOAD_MAX_BODY_SIZE  # 大于服务级 MAX_BODY_SIZE，需要放大才放行
        stream.feed(
            f"POST /audit/upload HTTP/1.1\r\nHost: 127.0.0.1\r\n"
            f"Content-Length: {big}\r\n\r\n".encode()
        )
        stream.feed(b"u" * big)
        await asyncio.sleep(0.05)
        self.assertEqual(server_delegate.delegates[0].finished, 1)
        self.assertFalse(stream.closed())

        # 同一连接上的第二个请求未做任何调整，必须被服务级小额度拦下。
        stream.feed(
            f"POST /auth HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {big}\r\n\r\n".encode()
        )
        await asyncio.sleep(0.05)
        self.assertTrue(stream.closed())
        self.assertIn(b"400 Bad Request", stream.write_buffer)


class WebHandlerTest(unittest.IsolatedAsyncioTestCase):
    """应用层：验证真实 Application 路由下 handler 调整的时机与隔离性。"""

    async def asyncSetUp(self) -> None:
        self.io_loop = IOLoop.current()
        self.app = build_app()

    def _start_request(self, stream: FakeStream, cp: HTTP1ConnectionParameters) -> Any:
        server_conn = HTTP1ServerConnection(stream, cp, context=type("Ctx", (), {})())
        server_conn.start_serving(self.app)
        return server_conn

    async def test_upload_route_raises_limit_but_auth_keeps_small(self) -> None:
        stream = FakeStream(self.io_loop)
        cp = params(body_timeout=UPLOAD_BODY_TIMEOUT)
        self._start_request(stream, cp)

        # 上传路由：超过服务级 64KiB 但在 2MiB 放大额度内，必须成功。
        payload = b"a" * (MAX_BODY_SIZE + 1024)
        stream.feed(
            (
                f"POST /audit/upload HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                f"Content-Length: {len(payload)}\r\n\r\n"
            ).encode()
        )
        stream.feed(payload)
        await asyncio.sleep(0.05)
        self.assertFalse(stream.closed())
        self.assertIn(b"200", stream.write_buffer)

        # 同一连接紧接着打认证路由，不允许继承上传路由的放大额度。
        auth_payload = b"b" * (MAX_BODY_SIZE + 1)
        stream.feed(
            f"POST /auth HTTP/1.1\r\nHost: 127.0.0.1\r\n"
            f"Content-Length: {len(auth_payload)}\r\n\r\n".encode()
        )
        await asyncio.sleep(0.05)
        self.assertTrue(stream.closed())
        self.assertIn(b"400 Bad Request", stream.write_buffer)

    async def test_auth_route_accepts_body_within_small_limit(self) -> None:
        stream = FakeStream(self.io_loop)
        self._start_request(stream, params())
        payload = b"token=abc"
        stream.feed(
            (
                f"POST /auth HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                f"Content-Length: {len(payload)}\r\n\r\n"
            ).encode()
        )
        stream.feed(payload)
        await asyncio.sleep(0.05)
        self.assertFalse(stream.closed())
        self.assertIn(b'"ok": true', stream.write_buffer)

    async def test_buffered_handler_cannot_raise_limit_in_time(self) -> None:
        """反证：非流式 handler 的调整点晚于协议层长度检查，必须用流式装饰器。"""

        class TooLate(AuthHandler):
            def prepare(self) -> None:
                # 缓冲式 handler 里即使调了也来不及：_read_body 已先跑完。
                self.request.connection.set_max_body_size(UPLOAD_MAX_BODY_SIZE)

        app = tornado.web.Application([(r"/late", TooLate)])
        stream = FakeStream(self.io_loop)
        server_conn = HTTP1ServerConnection(stream, params(), context=type("Ctx", (), {})())
        server_conn.start_serving(app)
        payload = b"c" * (MAX_BODY_SIZE + 1)
        stream.feed(
            (
                f"POST /late HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                f"Content-Length: {len(payload)}\r\n\r\n"
            ).encode()
        )
        await asyncio.sleep(0.05)
        self.assertTrue(stream.closed())
        self.assertIn(b"400 Bad Request", stream.write_buffer)

    async def test_application_phase_has_no_server_timeout(self) -> None:
        """覆盖盲区：请求读完、进入 handler 后，header/body timeout 都不再生效。

        handler 故意挂起不写响应。等待时长超过两个超时之和，连接仍保持打开，
        证明这一阶段只能靠应用自己的超时（或客户端断开）来兜底。
        """

        class HangingHandler(tornado.web.RequestHandler):
            async def post(self) -> None:
                # 永远不 finish，模拟应用处理卡死。
                await asyncio.Event().wait()

        app = tornado.web.Application([(r"/hang", HangingHandler)])
        stream = FakeStream(self.io_loop)
        server_conn = HTTP1ServerConnection(stream, params(), context=type("Ctx", (), {})())
        server_conn.start_serving(app)
        payload = b"x"
        stream.feed(
            (
                f"POST /hang HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                f"Content-Length: {len(payload)}\r\n\r\n"
            ).encode()
        )
        stream.feed(payload)
        # 等待超过 header_timeout(0.10) 与 body_timeout(0.15) 之和。
        await asyncio.sleep(T_HEADER + T_BODY + 0.1)
        self.assertFalse(stream.closed())
        self.assertEqual(bytes(stream.write_buffer), b"")  # 没有任何响应或关闭


if __name__ == "__main__":
    unittest.main(verbosity=2)
