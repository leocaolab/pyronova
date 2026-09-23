//! Cross-interpreter bridges.
//!
//! - `main_bridge`: sub-interpreter → main interpreter, for routes
//!   that must run on the main interp (C extensions, pydantic-core,
//!   numpy, etc. flagged with `gil=True`), via dedicated OS threads
//!   listening on an MPSC channel.
//!
//! Workers reach the database through the real `PgPool` (`db.rs`); the
//! C-FFI `db_bridge` they used while they ran a mock engine is gone (Layer 2).

pub(crate) mod main_bridge;
