#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod app;
#[cfg(feature = "bench")]
mod bench;
mod bridge;
mod compression;
mod db;
mod grpc;
mod handlers;
#[cfg(feature = "leak_detect")]
mod leak_detect;
mod logging;
mod monitor;
mod python;
mod request_id;
mod response;
mod router;
mod run_context;
mod server;
mod site;
mod state;
mod static_fs;
mod tls;
mod tpc;
mod types;
mod websocket;
mod worker;

use pyo3::prelude::*;

#[cfg(feature = "leak_detect")]
#[pyo3::pyfunction]
fn leak_detect_dump() {
    leak_detect::dump_to_stderr();
}

/// Panics with `message`. PyO3 turns the panic into a `PanicException` in the calling
/// handler, and the dispatcher that fetches it resumes the panic in Rust, on the path
/// that ran the handler.
#[cfg(feature = "fault_injection")]
#[pyo3::pyfunction]
fn _fault_panic(message: String) {
    panic!("{message}");
}

#[pyo3::pyfunction]
fn workrequest_counts() -> (u64, u64) {
    (
        python::interp::WorkRequest::created_count(),
        python::interp::WorkRequest::dropped_count(),
    )
}

/// Worker threads the last sub-interpreter pool shutdown abandoned, each with what it was
/// running; taking the list clears it. `Pyronova.run()` exits non-zero when it is not
/// empty, because finalizing with a live worker interpreter aborts (Layer 2, N8).
#[pyo3::pyfunction]
fn _forgotten_workers() -> Vec<String> {
    python::pool::take_forgotten_workers()
}

/// The parameter names of the route path `path`, in order (`{*rest}` gives `rest`). A path
/// the router would not take as written (`:name`) raises `ValueError`. The one parser of
/// route templates: path-param injection in `app.py` reads it, and route registration
/// runs it again.
#[pyo3::pyfunction]
fn _route_params(path: &str) -> PyResult<Vec<String>> {
    router::template_params(path)
        .map(|names| names.into_iter().map(str::to_owned).collect())
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("route {path}: {e}")))
}

/// Whether this code runs in a sub-interpreter worker, i.e. not in the main interpreter
/// (Layer 2, FR-15). Replaces a process-wide environment variable, which leaked into
/// child processes.
#[pyo3::pyfunction]
fn _in_worker(py: Python<'_>) -> bool {
    !run_context::on_main(py)
}

#[pymodule]
fn engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Remembers the main interpreter (no-op elsewhere), for threads that must attach to
    // it explicitly (Layer 2, C4).
    run_context::capture_main(m.py());
    m.add_class::<app::PyronovaApp>()?;
    m.add_class::<types::PyronovaRequest>()?;
    m.add_class::<types::PyronovaResponse>()?;
    m.add_class::<types::PyronovaHeaders>()?;
    m.add_class::<websocket::PyronovaWebSocket>()?;
    m.add_class::<state::SharedState>()?;
    m.add_class::<python::stream::PyronovaStream>()?;
    m.add_class::<python::body_stream::PyronovaBodyStream>()?;
    m.add(
        "BodyRejected",
        m.py().get_type::<python::body_stream::BodyRejected>(),
    )?;
    db::register(m)?;
    m.add_class::<monitor::Metrics>()?;
    m.add_function(pyo3::wrap_pyfunction!(monitor::get_gil_metrics, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(monitor::reset_peaks, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(logging::init_logger, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(logging::emit_python_log, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(workrequest_counts, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(_in_worker, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(_route_params, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(_forgotten_workers, m)?)?;
    // Called by the async engine in sub-interpreter workers (Layer 2, C5).
    m.add_function(pyo3::wrap_pyfunction!(python::worker_api::_worker_recv, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(python::worker_api::_worker_send, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(python::worker_api::_worker_fail, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        python::worker_api::_worker_timed_out,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        python::worker_api::_worker_to_response,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        python::worker_api::_worker_app_handlers,
        m
    )?)?;
    m.add_function(pyo3::wrap_pyfunction!(
        python::worker_api::_worker_app_hooks,
        m
    )?)?;
    #[cfg(feature = "leak_detect")]
    m.add_function(pyo3::wrap_pyfunction!(leak_detect_dump, m)?)?;
    #[cfg(feature = "fault_injection")]
    m.add_function(pyo3::wrap_pyfunction!(_fault_panic, m)?)?;
    Ok(())
}
