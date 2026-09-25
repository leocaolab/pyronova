//! Python runtime boundary — sub-interpreter workers + streaming glue between hyper and
//! Python.
//!
//! Grouping rationale: these modules hold the densest concentration of `unsafe` +
//! `pyo3::ffi::*` in the codebase. Everything else in the tree either uses PyO3's safe
//! bindings or has no PyO3 contact at all. Physically grouping the unsafe-heavy files
//! makes the FFI boundary easy to audit and isolate.
//!
//! - `worker`: `SubInterpreterWorker` — owns one sub-interpreter, its thread state and
//!   the references it serves with.
//! - `worker_app`: the app a worker's script registered on.
//! - `hook_chain`: one request's before hooks → handler → after hooks, shared by the main
//!   interpreter and the workers.
//! - `request_context`: one `contextvars.Context` per request.
//! - `pool`: `InterpreterPool`, `WorkRequest`, and the per-OS-thread worker loops.
//! - `worker_api`: the `#[pyfunction]`s the async engine calls (`_worker_recv` /
//!   `_worker_send` / ...), and its per-worker inbox.
//! - `body_stream`: hyper Request body → Python channel. Used by `stream=True` routes to
//!   feed upload data incrementally into a Python async generator.
//! - `stream`: Python channel → hyper Response body. Backs Server-Sent Events (SSE)
//!   responses.

pub(crate) mod body_stream;
pub(crate) mod hook_chain;
pub(crate) mod pool;
pub(crate) mod request_context;
pub(crate) mod stream;
pub(crate) mod worker;
pub(crate) mod worker_api;
pub(crate) mod worker_app;

/// Stack size for every thread that runs Python code.
///
/// Rust's `std::thread` default is 2 MiB; CPython's own threads get the
/// pthread default (8 MiB on Linux). C extensions are written against the
/// latter: OpenBLAS's `dgetrf_parallel` recurses with a large on-stack job
/// array, and on a 2 MiB pyronova worker `np.linalg.inv` under concurrent
/// load overflowed the stack (SIGSEGV in `dgetrf_parallel`, bluewhale,
/// numpy 2.5.1 / OpenBLAS 0.3.33). This is address space, not resident memory.
pub(crate) const PYTHON_THREAD_STACK: usize = 8 << 20;
