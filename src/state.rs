//! SharedState — cross-sub-interpreter state sharing via DashMap.
//!
//! All sub-interpreters share the same Arc<DashMap> in Rust memory.
//! Python code uses `app.state["key"] = value` / `app.state["key"]`.
//! Values stored as `bytes::Bytes` (ref-counted, zero-cost clone).

use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use pyo3::prelude::*;

/// High-concurrency shared key-value store backed by DashMap.
///
/// Thread-safe, lock-free reads for different keys, nanosecond latency.
/// All sub-interpreters share the same underlying DashMap via Arc.
/// Values are `Bytes` — clone is atomic refcount bump, not deep copy.
/// The map behind a `SharedState`.
pub(crate) type SharedMap = Arc<DashMap<String, Bytes>>;

/// The running app's map, handed to a worker before its script runs; one value per
/// interpreter (Layer 2, C2). Unset on main and in any interpreter pyronova didn't create.
static WORKER_MAP: pyo3::sync::PyOnceLock<SharedMap> = pyo3::sync::PyOnceLock::new();

/// Gives the worker interpreter the calling thread is attached to the running app's map.
pub(crate) fn hand_to_worker(py: Python<'_>, map: &SharedMap) -> Result<(), String> {
    // A worker interpreter is created for one run, so the cell must be empty here.
    WORKER_MAP
        .set(py, Arc::clone(map))
        .map_err(|_| "this worker interpreter already has a shared-state map".to_string())
}

/// The map a new `PyronovaApp` or `SharedState` uses: in a worker the running app's, so every
/// interpreter sees the same values (FR-5); otherwise a fresh one, as before (TestClient apps
/// in one process keep separate maps).
pub(crate) fn map_for_new(py: Python<'_>) -> SharedMap {
    match WORKER_MAP.get(py) {
        Some(map) => Arc::clone(map),
        None => Arc::new(DashMap::new()),
    }
}

#[pyclass(module = "pyronova.engine")]
pub(crate) struct SharedState {
    inner: SharedMap,
}

impl SharedState {
    /// Create a new SharedState with the given Arc (for sharing across workers).
    pub fn with_inner(inner: SharedMap) -> Self {
        SharedState { inner }
    }
}

#[pymethods]
impl SharedState {
    #[new]
    fn new(py: Python<'_>) -> Self {
        SharedState {
            inner: map_for_new(py),
        }
    }

    /// Set a string value.
    fn set(&self, key: String, value: String) {
        self.inner.insert(key, Bytes::from(value.into_bytes()));
    }

    /// Get a string value. Returns ``default`` (None) if key doesn't exist.
    #[pyo3(signature = (key, default=None))]
    fn get(&self, key: &str, default: Option<String>) -> Option<String> {
        self.inner
            .get(key)
            .and_then(|v| std::str::from_utf8(v.value()).ok().map(|s| s.to_string()))
            .or(default)
    }

    /// Set raw bytes value.
    fn set_bytes(&self, key: String, value: Vec<u8>) {
        self.inner.insert(key, Bytes::from(value));
    }

    /// Get raw bytes value. Copies the stored bytes into a new ``Vec<u8>``
    /// (required since the value is handed to Python as a ``bytes`` object).
    fn get_bytes(&self, key: &str) -> Option<Vec<u8>> {
        self.inner.get(key).map(|v| v.value().to_vec())
    }

    /// Delete a key. Returns True if it existed.
    fn delete(&self, key: &str) -> bool {
        self.inner.remove(key).is_some()
    }

    /// Get all keys.
    fn keys(&self) -> Vec<String> {
        self.inner.iter().map(|e| e.key().clone()).collect()
    }

    /// Get all string values (keys with non-UTF-8 bytes are silently skipped).
    fn values(&self) -> Vec<String> {
        self.inner
            .iter()
            .filter_map(|e| std::str::from_utf8(e.value()).ok().map(|s| s.to_string()))
            .collect()
    }

    /// Get all (key, value) pairs as a list of tuples.
    fn items(&self) -> Vec<(String, String)> {
        self.inner
            .iter()
            .filter_map(|e| {
                std::str::from_utf8(e.value())
                    .ok()
                    .map(|v| (e.key().clone(), v.to_string()))
            })
            .collect()
    }

    /// Number of entries.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Check if key exists.
    ///
    /// Mirrors `__getitem__`: a key only counts as "in" the state if its
    /// value is valid UTF-8, since `__getitem__` raises KeyError otherwise.
    /// This preserves the dict invariant that `key in state` implies
    /// `state[key]` succeeds. Use `get_bytes` for raw non-UTF-8 access.
    fn __contains__(&self, key: &str) -> bool {
        self.inner
            .get(key)
            .map(|v| std::str::from_utf8(v.value()).is_ok())
            .unwrap_or(false)
    }

    /// dict-like: state["key"] = "value"
    fn __setitem__(&self, key: String, value: String) {
        self.set(key, value);
    }

    /// dict-like: state["key"]
    ///
    /// Raises `KeyError` only when the key is genuinely absent. When the key
    /// exists but holds non-UTF-8 bytes (e.g. via `set_bytes`) this raises
    /// `TypeError` instead — masking it as `KeyError` would contradict
    /// `__contains__` (which returns True) and hide the fact that the value
    /// is present but not decodable as a string. Use `get_bytes` for raw access.
    fn __getitem__(&self, key: &str) -> PyResult<String> {
        match self.inner.get(key) {
            None => Err(pyo3::exceptions::PyKeyError::new_err(key.to_string())),
            Some(v) => std::str::from_utf8(v.value())
                .map(|s| s.to_string())
                .map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err(format!(
                        "state[{key:?}]: value is not valid UTF-8; use get_bytes() for raw access"
                    ))
                }),
        }
    }

    /// dict-like: del state["key"]
    fn __delitem__(&self, key: &str) -> PyResult<()> {
        if self.delete(key) {
            Ok(())
        } else {
            Err(pyo3::exceptions::PyKeyError::new_err(key.to_string()))
        }
    }

    /// Atomic increment: returns the new value. Creates key with `amount` if missing.
    ///
    /// If the existing value is not a valid UTF-8 integer this raises a
    /// TypeError instead of silently resetting to 0 — overwriting opaque
    /// bytes (e.g. a JSON blob someone stored with the same key) is a
    /// trap that can corrupt application state irrecoverably.
    fn incr(&self, key: String, amount: i64) -> PyResult<i64> {
        let mut entry = self
            .inner
            .entry(key.clone())
            .or_insert_with(|| Bytes::from("0"));
        let raw = std::str::from_utf8(entry.value()).map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err(format!(
                "incr({key:?}): existing value is not valid UTF-8"
            ))
        })?;
        let current: i64 = raw.parse().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err(format!(
                "incr({key:?}): existing value {raw:?} is not an integer"
            ))
        })?;
        let new_val = current.checked_add(amount).ok_or_else(|| {
            pyo3::exceptions::PyOverflowError::new_err(format!("incr({key:?}): i64 overflow"))
        })?;
        *entry = Bytes::from(new_val.to_string());
        Ok(new_val)
    }

    /// Atomic decrement: returns the new value.
    fn decr(&self, key: String, amount: i64) -> PyResult<i64> {
        let neg = amount.checked_neg().ok_or_else(|| {
            pyo3::exceptions::PyOverflowError::new_err("decr: amount i64::MIN cannot be negated")
        })?;
        self.incr(key, neg)
    }

    fn __repr__(&self) -> String {
        format!("SharedState({} keys)", self.inner.len())
    }
}
