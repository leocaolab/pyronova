# What Are Python Sub-Interpreters?

Python sub-interpreters are a way to run multiple, isolated Python
runtimes inside a single process. Each sub-interpreter has its own
modules, its own globals, and—since Python 3.12—its own Global
Interpreter Lock (GIL). That last property is what makes them
interesting: for the first time, a single Python process can execute
genuinely parallel Python code on multiple CPU cores, without spawning
separate OS processes.

This guide explains what sub-interpreters are, the four properties
that distinguish them from threads and from `multiprocessing`, how
they work together, how to implement them today, and the common
challenges teams hit when adopting them in production. See Pyronova,
a high-performance Python web framework built around sub-interpreters,
for a concrete reference implementation.

## What is a Python sub-interpreter?

A sub-interpreter is a fully independent Python interpreter created
inside an existing Python process via the C API call
`Py_NewInterpreter()`. It shares the host process's memory and CPython
binary, but maintains its own:

- module imports and module state
- built-in types and singletons
- exception state
- thread state (`PyThreadState`)
- reference-count bookkeeping
- since Python 3.12 (PEP 684): its own GIL

Sub-interpreters have existed since Python 1.5 (1997), but were
historically constrained: they shared a single GIL with the main
interpreter, which meant they couldn't actually run Python code in
parallel. That changed with **PEP 684 — A Per-Interpreter GIL**,
shipped in Python 3.12, and refined further in 3.13 and 3.14.

The promise is significant. Python's GIL has been the canonical
explanation for why CPU-bound Python doesn't scale across cores—and
why every team eventually reaches for `multiprocessing`, with all the
fork-vs-spawn quirks, IPC overhead, and shared-memory friction that
comes with it. Per-interpreter GIL changes the cost calculus: parallel
Python is now possible without leaving the process.

## What are the four properties of a sub-interpreter?

Sub-interpreters are best understood through four properties that
together distinguish them from threads, from `multiprocessing`, and
from coroutine-based concurrency.

### Isolation

Each sub-interpreter has its own module dictionary, its own globals,
and its own copy of every imported module. A change in one
sub-interpreter—mutating a class attribute, monkey-patching a module,
setting a global—is invisible to every other sub-interpreter.

Examples of what isolation buys you:

- A buggy handler that corrupts module state in one worker doesn't
  affect other workers
- Monkey-patching for tests can't leak across boundaries
- Configuration loaded per-interpreter stays per-interpreter
- A C extension's module-level state is per-interpreter, not global

Isolation answers questions like:

- Can I safely run untrusted plugins in the same process?
- Can I have per-tenant module state without spawning processes?
- Can a slow handler in worker A affect worker B?

The tradeoff: communication between sub-interpreters is intentionally
restricted. You can't simply pass a Python object from one
interpreter to another the way threads share memory. Cross-interpreter
data has to be transferred through explicit channels, byte buffers, or
shared atomic primitives.

### Parallelism

Since Python 3.12, each sub-interpreter has its own GIL. That means
N sub-interpreters can execute Python bytecode on N CPU cores
simultaneously—true parallel CPU-bound Python in a single process.

Examples of where this matters:

- HTTP servers handling thousands of requests per second on every core
- Data pipelines that need to parse, transform, and validate in
  parallel without `multiprocessing` overhead
- Long-running services that want to scale CPU-bound work without
  fork/spawn
- ML inference servers running parallel preprocessing in pure Python

Parallelism answers questions like:

- Can my Python web server use all 16 cores without `gunicorn` workers?
- Can I parallelize CPU-bound work without IPC overhead?
- Can I scale Python compute on a single VM?

The tradeoff: parallelism only helps for CPU-bound workloads.
I/O-bound workloads (database calls, HTTP requests) gain little
because they already release the GIL during system calls. And the
parallelism is per-interpreter, not free-threading—objects are still
not shared.

### Memory safety

Every sub-interpreter has its own reference-counting bookkeeping. An
object created in interpreter A cannot be referenced from interpreter
B without an explicit transfer mechanism. CPython enforces this
through `PyThreadState`, which binds the current execution context to
a specific interpreter.

Examples of what memory safety buys you:

- No race conditions on shared Python objects
- Garbage collection runs per-interpreter, on its own schedule
- Memory leaks in one interpreter are bounded to that interpreter's
  arena
- Killing a stuck sub-interpreter doesn't corrupt others

Memory safety answers questions like:

- Can I isolate a misbehaving handler without process-level overhead?
- Can I bound memory pressure per tenant?
- Can I cleanly shut down one worker without restarting the process?

The tradeoff: object transfer between sub-interpreters requires
explicit serialization. There's no shared `dict` between interpreters.
Working channels and shared state require careful design.

### C-extension compatibility

This is the messiest property. C extensions compiled against an older
CPython API often hold global state that violates per-interpreter
isolation: a single `static PyObject* my_module_state` in C code
becomes a process-wide bug under sub-interpreters.

PEP 630 (Isolating Module Objects) and the Multi-Phase Initialization
API (PEP 489) define the path forward, but adoption is uneven.

Examples of what works well:

- Pure-Python libraries: full sub-interpreter compatibility
- Modern C extensions using multi-phase init: full compatibility
- `numpy`: experimental sub-interpreter support since 1.26
- Most stdlib modules: tested and working

Examples of what's still problematic:

- Older C extensions with module-level globals
- C extensions doing `PyEval_GetGlobals()` assumptions
- Some scientific libraries (mature ones added support; long tail
  hasn't)
- Anything embedding the interpreter in unusual ways

C-extension compatibility answers questions like:

- Can I use `numpy` / `pandas` / `pydantic` in sub-interpreters?
- Will my legacy C library break things?
- How do I detect incompatibility ahead of time?

The tradeoff: this is the property most likely to surprise you in
production. Test C extensions in sub-interpreter mode before relying
on them.

## How do these four properties work together?

The properties are useful in isolation, but they unlock a new class of
architecture when combined.

Consider a high-throughput Python web server—the use case Pyronova
was built for.

- **Isolation** means each worker can hold its own request-scoped
  state, route table, and prepared statement cache without leaking
  into other workers.
- **Parallelism** means N workers serve requests on N cores in
  parallel, without any process-fork overhead. A 16-core machine
  becomes a true 16-core Python server.
- **Memory safety** means a memory leak in one handler stays in one
  worker's arena. The framework can monitor per-interpreter RSS and
  recycle a worker that grows without bound—isolated from the rest.
- **C-extension compatibility** determines what the workers can
  actually run. Pure-Python and modern numerical workloads work
  out of the box; older C extensions need a "GIL route" fallback
  that dispatches to the main interpreter, which still holds the
  legacy GIL.

The architecture that emerges:

1. Process boots, creates N sub-interpreters (one per CPU core)
2. Each sub-interpreter loads the application code independently
3. Incoming HTTP requests are dispatched to a sub-interpreter via a
   lock-free channel
4. The handler runs in the sub-interpreter, in parallel with handlers
   in other sub-interpreters
5. Response is sent back through the channel
6. Periodic health checks verify per-interpreter RSS and restart any
   that drift

This is fundamentally different from `gunicorn -w N` (multiple
processes) or threaded servers (one GIL bottleneck). Pyronova's
benchmarks on a Ryzen 7 7840HS (8 cores, 16 threads) show 422,976
requests per second on the plaintext baseline at v1.5.0—something
that would require 8+ Python processes to achieve under traditional
architectures.

## Implementing sub-interpreters in your application

Adopting sub-interpreters in production typically involves five
steps.

1. **Verify your Python version.** Per-interpreter GIL requires
   Python 3.12 or later. Many of the rough edges have been smoothed
   in 3.13 and 3.14; if you're starting fresh, target 3.13+ to avoid
   known issues with `PyThreadState` lifecycle.

2. **Audit your dependencies for C-extension compatibility.** This
   is the step most likely to derail adoption. Build a small test
   harness that imports each dependency in a sub-interpreter and
   exercises its API. Common stdlib works; `numpy`, `pandas`,
   `pydantic` work in recent versions; older bindings may not.
   Document a list of known-incompatible libraries before you build
   on them.

3. **Design your communication primitives.** Decide how data will
   move between the main interpreter and sub-interpreters. Options
   include the `interpreters` stdlib module (Python 3.13+), explicit
   `crossbeam-channel`-style queues built into a Rust extension,
   shared-memory primitives via `multiprocessing.shared_memory`, or
   pure byte transfers. Pyronova's Rust engine hands each worker its
   requests over channels, and its extension module loads in every
   worker (one module object per interpreter), so a worker calls the
   engine's functions directly without crossing GILs.

4. **Plan for graceful degradation.** Some routes or operations will
   require the main interpreter (legacy C extensions, GIL-only
   libraries). Design a "GIL route" path that dispatches these to a
   single main-interpreter thread. In Pyronova, this is the
   `gil=True` decorator flag on routes.

5. **Instrument per-interpreter metrics.** Track memory, request
   count, error rate, and GIL contention per sub-interpreter—not
   just per-process. Per-interpreter visibility is what lets you
   detect a misbehaving worker and recycle it without restarting
   the process.

The most important step is #2. Teams routinely underestimate the
C-extension compatibility surface and then discover late that a
critical library blocks adoption.

## Common challenges with sub-interpreters

Even when sub-interpreters work, several challenges recur in
production deployments.

- **C-extension compatibility surprises.** A library that worked in
  testing fails under sustained load because of a hidden global. The
  fix is usually to file an upstream issue and either pin to a
  compatible version or route those calls through a GIL fallback.

- **Cross-interpreter object transfer overhead.** You can't just pass
  a `dict` between interpreters. Serializing a complex Python object
  to bytes, sending it across a channel, and deserializing on the
  other side is fast but not free. For hot paths, design your data
  flow to avoid object transfer entirely—pass byte buffers and
  parse on the receiving side.

- **Memory leaks that don't look like leaks.** Pyronova's v1.5.0
  release closed a long-running memory growth bug that turned out to
  be cross-thread `PyThreadState` reuse: a worker thread would call
  into a sub-interpreter using a `PyThreadState` created on a
  different thread, leaking ~128 bytes per request at 400k req/s.
  The fix was creating `PyThreadState_New` per worker thread.
  Diagnostics like this are subtle and require per-interpreter
  instrumentation to spot.

- **Debugging tooling lag.** Many existing Python debuggers, profilers,
  and observability tools assume a single global interpreter. Some
  break under sub-interpreters; others give misleading results. Verify
  your tooling stack early.

- **Async runtime confusion.** If you're using `asyncio`, each
  sub-interpreter has its own event loop. Cross-interpreter `await`
  doesn't exist. Design your async architecture so that an entire
  request stays within one interpreter, and cross-interpreter
  communication happens at the bytes-and-channels level.

- **A private copy for incompatible libraries.** A C extension that
  keeps process-global state (numpy, orjson, pydantic-core, polars)
  can't be shared between interpreters. Pyronova gives each worker its
  own copy of such a library and initializes it inside that worker, so
  the real library runs in every worker instead of a stand-in. Earlier
  versions stubbed out parts of `pydantic` and Pyronova's own modules;
  those stubs are gone.

## From sub-interpreters to multi-core Python

Sub-interpreters are one of three approaches Python is taking to
escape the GIL bottleneck. Understanding how they relate clarifies
when to use which.

- **Sub-interpreters (PEP 684, Python 3.12+):** Multiple isolated
  Python runtimes in one process, each with its own GIL. Best for
  parallel server-style workloads where isolation is a feature.
- **Free-threading (PEP 703, Python 3.13+ experimental):** A single
  Python interpreter with no GIL at all. Best for shared-memory
  parallel computation. Still experimental and has performance costs
  on single-threaded workloads.
- **`multiprocessing`:** Multiple OS processes, each with its own
  Python interpreter. Best for fully independent workloads where IPC
  cost is acceptable.

Sub-interpreters fall in between. They're cheaper to start than
processes (no fork, no exec), they share a single binary and OS
resources, and they're isolated enough to recover from failures. For
HTTP servers, task workers, and pipeline stages, they're often the
best fit.

If you've been working around the GIL with `gunicorn -w N` or
`uvicorn --workers N`, sub-interpreters offer a simpler operational
model: one process, one set of file descriptors, one set of metrics—
but with N parallel cores' worth of throughput.

## See sub-interpreters in production

Pyronova is a Python web framework built from the ground up around
sub-interpreters. It uses per-interpreter GIL for parallelism, loads
its Rust extension in every worker for native async without GIL
crossing, and
mimalloc as a global allocator to keep per-interpreter memory
overhead low. Benchmarks on commodity hardware exceed 420,000
requests per second on the plaintext baseline—numbers that
traditionally required 8+ separate Python processes to achieve.

If you're evaluating sub-interpreters for a real workload, Pyronova
is one of the few production-tested reference implementations. See
the project on [GitHub](https://github.com/leocaolab/pyronova) and
the architecture documentation for details.

## Frequently asked questions

### What is the difference between a sub-interpreter and a thread?

Threads share a single Python interpreter and a single GIL—only one
thread can execute Python bytecode at a time. Sub-interpreters each
have their own interpreter and (since Python 3.12) their own GIL,
allowing N sub-interpreters to execute Python in parallel on N cores.

### Do sub-interpreters replace `multiprocessing`?

Not entirely. `multiprocessing` gives you full process isolation
(separate memory, separate file descriptors, immune to corruption in
peers). Sub-interpreters give you Python-level isolation in a single
process, with much lower startup cost and operational overhead. For
many web-server and worker workloads, sub-interpreters are now the
better fit; for fully independent batch jobs, `multiprocessing` is
still common.

### Which Python version do I need?

Per-interpreter GIL requires Python 3.12 or later. Production
deployments are advised to target 3.13 or 3.14, where many initial
rough edges have been resolved.

### Can I use `numpy`, `pandas`, or `pydantic` in a sub-interpreter?

Recent versions, yes. `numpy` 1.26+ has experimental sub-interpreter
support, and modern `pandas` and `pydantic` builds work. Older
versions or extensions with global state may not. Always test in a
sub-interpreter harness before adopting.

### How do sub-interpreters communicate?

Through explicit channels, byte buffers, or shared-memory primitives.
Python 3.13's stdlib `interpreters` module provides built-in
queue-based communication. Frameworks like Pyronova add native
high-performance channels via Rust extensions.

### Are sub-interpreters faster than threads?

For CPU-bound Python code, yes—dramatically. A 16-core machine can
execute up to 16x more parallel Python work in sub-interpreters than
in threads, because threads share a single GIL while sub-interpreters
each have their own. For I/O-bound code, the difference is smaller.

### What is PEP 684?

PEP 684 ("A Per-Interpreter GIL") is the Python proposal that gave
each sub-interpreter its own GIL. It was accepted and shipped in
Python 3.12 (October 2023). It's the change that makes
sub-interpreters genuinely useful for parallelism.

### Are sub-interpreters production-ready?

In Python 3.13+, yes—for workloads where you've verified C-extension
compatibility and built around the isolation model. Production
deployments exist (Pyronova being one open-source reference). The
ecosystem of compatible C extensions is still expanding, so adoption
involves some library auditing.
