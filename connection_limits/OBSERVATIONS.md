# 连接限制生效边界观察表

基于本仓库 Tornado 6.7.dev1 源码核实，配套验证脚本
`connection_limits/verify_limits.py`（11 个用例，内存可控 IO，无端口、无压测，
整体运行约 1 秒）。所有行号对应当前工作区源码。

## 一、参数与实际生效位置

| HTTPServer 参数 | 实际落点 | 默认值 | 作用阶段 |
| --- | --- | --- | --- |
| `idle_connection_timeout` | 映射为 `HTTP1ConnectionParameters.header_timeout`（`tornado/httpserver.py:189`） | 3600s（`or 3600`，传 0 会退回默认） | 仅“等待完整请求头” |
| `body_timeout` | `HTTP1Connection._body_timeout`（`tornado/http1connection.py:165`） | `None`（不限时） | 仅“读取整个正文” |
| `max_header_size` | `HTTP1ConnectionParameters.max_header_size`（默认 65536），作为 `read_until_regex(..., max_bytes=)` 上限（`http1connection.py:226-228`） | 64 KiB | 请求头读取 |
| `max_body_size` | `HTTP1Connection._max_body_size`；未显式给定时退化为 `stream.max_buffer_size`（`http1connection.py:160-164`） | 100 MiB（IOStream 默认） | 正文长度计数 |
| `max_buffer_size`（TCPServer） | `IOStream.max_buffer_size`（`tornado/iostream.py:265`），底层读缓冲硬上限 | 100 MiB | 所有读操作的缓冲 |

## 二、逐项边界与关闭结果

| 场景 | 触发的限制 | 计时/计数边界（源码依据） | 关闭结果 | 对应用例 |
| --- | --- | --- | --- | --- |
| 只送半个请求头后静默 | `header_timeout` | 从发起 `read_until_regex` 起算的一次性截止时间（`http1connection.py:233-240`）；**头部块之间没有逐字节空闲重置** | 到期 `self.close()` 静默关连接，**不写 400**，也不回调 delegate 的 `on_connection_close`（此时 `need_delegate_close=False`） | `test_half_header_idle_hits_header_timeout` |
| 请求头超过 `max_header_size` | `max_header_size` | IOStream 每读到数据就在 `_find_read_pos/_check_max_bytes` 比较，命中分隔符时用命中位置长度、未命中用当前缓冲长度（`iostream.py:976-994`） | 抛 `UnsatisfiableReadError` → 关连接，**无 400 响应**（区别于正文超限） | `test_header_oversize_closes_without_400` |
| 慢正文（头部完整、正文拖时间） | `body_timeout` | `_read_body` 返回的整个 future 被一个 `gen.with_timeout` 包裹（`http1connection.py:309-321`）。是**正文总时长上限，不是块间空闲超时**，中途收到数据不重置计时 | 到期日志 `Timeout reading body` 并 `stream.close()`，无响应；已收的部分正文仍会交付 `data_received`，但不会 `finish()` | `test_slow_body_hits_body_timeout_total_deadline` |
| Content-Length 超 `max_body_size` | `max_body_size` | 在读任何正文字节**之前**预检查 `content_length > _max_body_size`（`http1connection.py:673-674`） | 抛 `HTTPInputError("Content-Length too long")` → 写出 `400 Bad Request` 后关连接（`http1connection.py:343-352`） | `test_content_length_over_limit_returns_400_before_body` |
| chunked 正文累计超限 | `max_body_size` | 按每个 chunk 的载荷长度累加 `total_size`，超过即判失败（`http1connection.py:765-767`）；不计 chunk 帧头开销 | `400 Bad Request` 后关连接 | `test_chunked_body_over_limit_returns_400` |
| keep-alive 空闲 | `header_timeout`（同一个参数） | 上一请求的响应写完、`_finish_future` resolve 后，服务循环 `_server_request_loop` 新建 `HTTP1Connection` 并再次等头部（`http1connection.py:946-967`）。新一轮的头部等待重新受 `header_timeout` 约束 | 到期静默关连接，无 400；第二个 delegate 虽已创建，但 `headers_received` 从未触发 | `test_keepalive_idle_uses_header_timeout_per_request` |
| 请求已完整、进入应用处理 | **无服务端超时覆盖** | 正文读完后 `_read_finished=True`，代码进入 `await self._finish_future` 等应用响应（`http1connection.py:332-338`），此段不再挂任何 timeout | 连接保持，直到应用自己结束或客户端断开 | `test_application_phase_has_no_server_timeout` |

## 三、每请求调整为何不残留

- `HTTP1ServerConnection._server_request_loop` 每个请求都 `new HTTP1Connection(...)`
  （`http1connection.py:951`）。
- 新连接对象在构造时从 `params` **复制**初始 `_max_body_size` 与 `_body_timeout`
  （`http1connection.py:160-165`），不是共享可变引用。
- 因此 handler 内 `set_max_body_size` / `set_body_timeout`
  （`http1connection.py:422-434`）只改当前请求对象；下一请求天然回到服务级默认，
  无需手工还原。用例 `test_per_request_override_does_not_leak` 与
  `test_upload_route_raises_limit_but_auth_keeps_small` 分别在协议层与真实
  Application 路由层验证了这一点。

## 四、handler 调整的生效时机（为什么必须用流式装饰器）

- `set_max_body_size` 必须赶在 `_read_body` 的 Content-Length 预检查
  （`http1connection.py:673`）之前才有效。
- 缓冲式 handler 的 handler 对象在整个正文读完后的 `_HandlerDelegate.finish`
  才创建（`tornado/web.py:2640-2647`），此时再调整已经晚了。
- `@stream_request_body` 的 handler 在 `headers_received` 阶段就执行
  `prepare()`（`web.py:2628-2630`、`_execute` 中先 `prepare` 再等 `_body_future`），
  早于任何 `data_received`，调整能赶上预检查与累计计数。
- 反证用例 `test_buffered_handler_cannot_raise_limit_in_time` 确认：缓冲式
  handler 即使在 `prepare` 里调 `set_max_body_size`，仍被服务级小额度 400 拦下。

## 五、参数覆盖不到的等待阶段（不夸大全链路防护）

1. **应用处理时间**：请求读完到响应写完之间，HTTPServer 没有任何超时；
   `body_timeout` 不是处理超时。需要在 handler 内用 `gen.with_timeout` /
   `asyncio.wait_for` 自行约束下游调用。
2. **TLS 握手**：发生在 `handle_stream` 之前，不在上述任何 HTTP 参数范围内，
   需由前置代理或 SSL 层参数控制。
3. **响应回写慢（客户端收得慢）**：受 `IOStream.max_buffer_size` 缓冲约束，
   但没有时间维度的超时；慢客户端可能长期占着连接等写排空。
4. **`body_timeout` 是总时长而非空闲时长**：它无法区分“持续低速上传”与
   “中途停顿”，只能按总预算切断。要防块间停顿需在流式 `data_received` 里
   自行打点计时。
5. **头部处理耗时**：`headers_received` 委托调用本身（`http1connection.py:259-262`）
   不在 header_timeout 计时保护内，timeout 只覆盖“等头部字节到达”。
