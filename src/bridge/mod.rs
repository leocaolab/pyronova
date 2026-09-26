//! Cross-interpreter bridges.
//!
//! - `main_bridge`: TPC threads → main interpreter, for `gil=True` routes (C extensions
//!   that can't load in a sub-interpreter), via dedicated OS threads on a bounded channel.

pub(crate) mod main_bridge;
