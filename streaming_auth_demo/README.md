# Streaming upload + auth: execution order investigation

Question: with `@stream_request_body` and `@authenticated` on `post`,
`data_received` writes chunks into a staging area *before* any identity
check runs. Is the framework violating its lifecycle, and how should the
app wire auth so no business data is received before auth succeeds?

## Verdict

Application-level misplacement, **not** a framework bug. No library
change is needed. Tornado gates body delivery on `prepare()`; the check
was placed on `post()`, which by design runs after the whole body.

## The framework's guaranteed order (Tornado 6.7.dev1, this repo)

1. `HTTP1Connection._read_message_inner` reads the headers, calls
   `delegate.headers_received()` and **awaits it before reading a single
   body byte** (tornado/http1connection.py:260-262; body read starts at
   tornado/http1connection.py:304).
2. For a streaming handler, `headers_received` runs through
   `_RoutingDelegate` -> `_HandlerDelegate.execute()`, which starts
   `RequestHandler._execute` and returns `handler._prepared_future`
   (tornado/web.py:2623-2631, tornado/web.py:2674-2690).
3. `_execute` awaits `prepare()` (it may be a coroutine) and only then
   resolves `_prepared_future` (tornado/web.py:1987-1993). If `prepare`
   raises, the `finally` at tornado/web.py:2039-2043 still resolves it
   so the connection can clean up.
4. Only after that does the connection read body chunks and call
   `handler.data_received(chunk)` -- and only while no response has been
   sent yet (`if not self._write_finished`,
   tornado/http1connection.py:698-705). After a rejection, remaining
   chunks are read and **discarded**, never delivered.
5. When the body is complete, `_HandlerDelegate.finish()` resolves
   `request._body_future`, `_execute` resumes and calls `post()`
   (tornado/web.py:2002-2008, tornado/web.py:2640-2642).
6. If the response finished before the request body was fully read,
   `HTTP1Connection.finish()` sets `_disconnect_on_finish` and closes
   the connection after the response (tornado/http1connection.py:574-580,
   tornado/http1connection.py:622-626). Leftover bytes can never be
   re-parsed as another request on the same connection.
7. `on_connection_close` fires on a streaming handler when the
   connection closes while its delegate is still active
   (tornado/http1connection.py:354-357 -> tornado/web.py:2649-2651).

So the guaranteed order is:

    prepare (fully awaited) -> data_received* -> post

and `@authenticated` on `post` cannot help: it runs at step 5, after the
whole body. It also requires a *synchronous* `get_current_user`
(tornado/web.py:1499-1501), so an async identity check cannot even be
wired into it. The documented place for an async check that must gate
the body is `prepare()`.

## The fix (fixed_app.py)

```python
@tornado.web.stream_request_body
class UploadHandler(BaseUploadHandler):
    async def prepare(self):
        self.current_user = await self.check_auth()   # pausable, async
        if self.current_user is None:
            raise tornado.web.HTTPError(403)

    @tornado.web.authenticated
    async def post(self):   # unchanged, no extra check added
        ...
```

No re-check at the top of `post` (the existing `@authenticated` simply
passes because `prepare` already set `current_user`), and no full
buffering -- `data_received` still streams chunk by chunk, it is just
never called for unauthorized requests.

## Observed callback orders

Run: `.venv/bin/python streaming_auth_demo/run_scenarios.py`

(The sandbox forbids binding listeners, so the harness hands a
`socketpair()` to `tornado.http1connection.HTTP1ServerConnection` -- the
same class TCPServer uses for accepted connections.)

### repro_app.py (check on `post` via `@authenticated`) -- the bug

```
denied:        prepare -> data_received x3 (99B staged) -> get_current_user -> 403
               (no early rejection: client had to send the whole body first)
check raises:  prepare -> data_received x2 (64B staged) -> get_current_user raises -> 500
check pending: prepare -> data_received x2 (staged while "pending") -> get_current_user -> 403/200
normal:        prepare -> data_received x2 -> get_current_user -> post -> 200
```

Staging writes for unauthorized requests: **3 and 2** (the leak).

### fixed_app.py (check in async `prepare`) -- the fix

```
denied:        prepare -> auth:start -> auth:result None -> 403 -> on_connection_close
               staging writes: 0. Client still owed 35 body bytes + sent a
               smuggled "GET /smuggled" afterwards: discarded, connection
               closed (EOF), no second request parsed.
check raises:  prepare -> auth:start -> 500 -> on_connection_close
               staging writes: 0
check pending: prepare -> auth:waiting (client sends 64B meanwhile; staging
               stays 0) -> auth:resumed -> 403 -> on_connection_close
               staging writes: 0
pending -> ok: prepare -> auth:waiting -> auth:resumed -> auth:result
               -> data_received (64B) -> post -> 200
normal:        prepare -> auth -> data_received x2 -> post -> 200
```

Staging writes for unauthorized requests: **0** in every scenario.

Note: in "pending -> ok" the two 32-byte chunks were coalesced into one
64-byte `data_received` -- chunk boundaries are not preserved
(tornado/http1connection.py:722-792), only ordering is.

## Files

- `repro_app.py`   -- minimal app reproducing the bug (`:8891`)
- `fixed_app.py`   -- minimal fixed app (`:8892`)
- `common.py`      -- pausable check, staging area, event log
- `run_scenarios.py` -- harness; exits non-zero if any check fails
