//! Python runtime boundary — sub-interpreter management + streaming
//! glue between hyper and Python.
//!
//! Grouping rationale: these three modules hold the densest
//! concentration of `unsafe` + `pyo3::ffi::*` in the codebase.
//! Everything else in the tree either uses PyO3's safe bindings
//! (`Python::attach`, `Py<PyAny>`, #[pyclass] getters) or has no
//! PyO3 contact at all. Physically grouping the unsafe-heavy files
//! makes the FFI boundary easy to audit and isolate.
//!
//! Sub-interpreter management was historically one ~2.7k-LOC `interp`
//! god module. It is now split into cohesive siblings (all still
//! unsafe-heavy, so the audit-isolation rationale above still holds):
//!
//! - `ffi`: raw FFI primitives — `PyObjRef` RAII, `SubInterpGilGuard`,
//!   tstate rebinding, the worker-state registry, and the C-FFI bridge
//!   (`pyronova_recv`/`pyronova_send`/`pyronova_emit_log`).
//! - `convert`: Python `str`/`dict` conversion helpers.
//! - `worker`: `SubInterpreterWorker` — owns one sub-interpreter.
//! - `pool`: `InterpreterPool`, `WorkRequest`, `SubInterpResponse`, and
//!   the per-OS-thread worker loops.
//! - `interp`: thin facade re-exporting the four above so existing
//!   `crate::python::interp::X` call sites keep compiling unchanged.
//! - `body_stream`: hyper Request body → Python channel. Used by
//!   `stream=True` routes to feed upload data incrementally into
//!   a Python async generator.
//! - `stream`: Python channel → hyper Response body. Backs
//!   Server-Sent Events (SSE) responses.
//!
//! Exports the previous crate-root module paths by re-exporting
//! as `pub(crate) use`, so existing `crate::interp::X`-style call
//! sites keep compiling with `crate::python::interp::X`.

pub(crate) mod body_stream;
pub(crate) mod convert;
pub(crate) mod ffi;
pub(crate) mod interp;
pub(crate) mod pool;
pub(crate) mod stream;
pub(crate) mod worker;
