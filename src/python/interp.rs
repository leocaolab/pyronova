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
//!     tstate rebinding, the worker-state registry, and the C-FFI bridge).
//!   - [`super::convert`] — Python `str`/`dict` conversion helpers.
//!   - [`super::worker`]  — `SubInterpreterWorker` (owns one sub-interpreter).
//!   - [`super::pool`]    — `InterpreterPool`, `WorkRequest`,
//!     `SubInterpResponse`, and the per-thread worker loops.
//!
//! Everything is re-exported here so existing `crate::python::interp::X`
//! call sites keep compiling unchanged.

// `convert` helpers are consumed by the sibling modules directly rather
// than through this facade, but they are re-exported here too so the
// `interp::` namespace stays a complete, symmetric view of the split.
#[allow(unused_imports)]
pub(crate) use super::convert::*;
pub(crate) use super::ffi::*;
pub(crate) use super::pool::*;
pub(crate) use super::worker::*;
