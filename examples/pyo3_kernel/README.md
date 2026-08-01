# pyo3_kernel — example sub-interpreter-safe native kernel (PyO3 0.29)

Build & install into your Pyronova venv, then run `../c_extension_subinterp.py`:

```bash
cd examples/pyo3_kernel
PYO3_PYTHON=../../.venv/bin/python cargo build --release
cp target/release/libpyo3_kernel.dylib ../../.venv/lib/python3.14/site-packages/pyo3_kernel.so   # macOS
# Linux: cp target/release/libpyo3_kernel.so .../site-packages/pyo3_kernel.so
```

`apply(inbuf, outbuf)` runs a zero-copy f64 UDF over two memoryviews; `Thing` is a
`#[pyclass]` demonstrating that PyO3 0.29 registers classes inside sub-interpreters
(which 0.28 could not). See `docs/subinterp-c-extension-status.md`.
