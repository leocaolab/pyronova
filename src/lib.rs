#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod app;
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
mod response;
mod router;
mod server;
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

#[pyo3::pyfunction]
fn workrequest_counts() -> (u64, u64) {
    (
        python::interp::WorkRequest::created_count(),
        python::interp::WorkRequest::dropped_count(),
    )
}

#[pymodule]
fn engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<app::PyronovaApp>()?;
    m.add_class::<types::PyronovaRequest>()?;
    m.add_class::<types::PyronovaResponse>()?;
    m.add_class::<websocket::PyronovaWebSocket>()?;
    m.add_class::<state::SharedState>()?;
    m.add_class::<python::stream::PyronovaStream>()?;
    m.add_class::<python::body_stream::PyronovaBodyStream>()?;
    m.add_class::<db::PgPool>()?;
    m.add_class::<db::PgCursor>()?;
    m.add_function(pyo3::wrap_pyfunction!(monitor::get_gil_metrics, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(logging::init_logger, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(logging::emit_python_log, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(workrequest_counts, m)?)?;
    #[cfg(feature = "leak_detect")]
    m.add_function(pyo3::wrap_pyfunction!(leak_detect_dump, m)?)?;
    Ok(())
}
