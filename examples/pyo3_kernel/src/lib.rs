use pyo3::prelude::*;
use pyo3::buffer::PyBuffer;

#[pyfunction]
fn apply(inbuf: PyBuffer<f64>, outbuf: PyBuffer<f64>) -> PyResult<()> {
    let n = inbuf.item_count();
    let x = inbuf.buf_ptr() as *const f64;
    let y = outbuf.buf_ptr() as *mut f64;
    unsafe {
        let x = std::slice::from_raw_parts(x, n);
        let y = std::slice::from_raw_parts_mut(y, n);
        for i in 0..n { let v = x[i]; y[i] = (v*v+1.0).sqrt()*0.5 + v.sin()*v.cos(); }
    }
    Ok(())
}

#[pyclass]
struct Thing { #[pyo3(get)] v: i64 }
#[pymethods]
impl Thing {
    #[new]
    fn new(v: i64) -> Self { Thing { v } }
}

#[pymodule]
fn pyo3_kernel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(apply, m)?)?;
    m.add_class::<Thing>()?;
    Ok(())
}
