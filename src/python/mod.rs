//! The Python side of request handling: sub-interpreter workers and their pool, the
//! hook → handler → hook chain, per-request `contextvars` contexts, and the channels that
//! stream request and response bodies between hyper and Python.
//!
//! The raw FFI that creates, enters and ends sub-interpreters lives in `worker`; the other
//! modules here go through PyO3's API.

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
/// Rust's `std::thread` default is 2 MiB; CPython's own threads get the pthread default
/// (8 MiB on Linux), which C extensions are written against: OpenBLAS's
/// `dgetrf_parallel` keeps a large job array on the stack and overflows 2 MiB under
/// `np.linalg.inv` (numpy 2.5.1 / OpenBLAS 0.3.33). Address space, not resident memory.
pub(crate) const PYTHON_THREAD_STACK: usize = 8 << 20;
