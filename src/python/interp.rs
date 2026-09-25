//! Safe abstractions for CPython sub-interpreter management.
//!
//! Provides RAII wrappers over raw `pyo3::ffi` pointers to prevent
//! reference count leaks and ensure proper sub-interpreter cleanup.
//! Also implements a channel-based worker pool for true load balancing.
//!
//! This module is now a thin facade: the implementation was split into
//! cohesive sibling modules to tame what had grown into a ~2.7k-LOC god
//! module. The split keeps the unsafe-heavy FFI surface auditable while
//! separating orthogonal concerns:
//!
//!   - [`super::ffi`]     — raw FFI primitives (`PyObjRef`, `SubInterpGilGuard`,
//!     tstate rebinding, and the async worker-state registry).
//!   - [`super::worker_api`] — the engine functions the async engine calls
//!     (`_worker_recv`, `_worker_send`, ...).
//!   - [`super::worker`]  — `SubInterpreterWorker` (owns one sub-interpreter).
//!   - [`super::pool`]    — `InterpreterPool`, `WorkRequest`, and the per-thread
//!     worker loops.
//!
//! Everything is re-exported here so existing `crate::python::interp::X`
//! call sites keep compiling unchanged.

pub(crate) use super::ffi::*;
pub(crate) use super::pool::*;
pub(crate) use super::worker::*;
