---
paths:
  - "src/**/*.py"
  - "tests/**/*.py"
---

# Async programming rule

The write/read paths are async end-to-end. Keep them non-blocking.

- **No blocking calls in async functions** — no synchronous file I/O, no `time.sleep`,
  no blocking DB/network calls inside `async def`. Ruff `ASYNC` flags the common cases.
- **Offload CPU/blocking work** with `anyio.to_thread.run_sync` (or the established
  helper) rather than blocking the event loop.
- **Concurrency** via `asyncio.gather` / `asyncio.TaskGroup` for independent awaits;
  don't `await` in a loop when the calls are independent.
- **Tests**: `pytest-asyncio` is in `auto` mode — an `async def test_*` just works,
  no `@pytest.mark.asyncio` needed.
- **Don't fire-and-forget** without holding a reference (`asyncio.create_task` results
  must be tracked, or you lose exceptions). The OME subsystem owns the long-running
  background loops — application code shouldn't spawn its own.
