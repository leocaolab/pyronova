//! Network- and host-layer primitives: TCP listener setup, accept-error handling,
//! platform-specific socket options, CPU counting and pinning.

pub(crate) mod cpu;
pub(crate) mod listener;
