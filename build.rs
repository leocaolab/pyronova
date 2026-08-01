fn main() {
    // These Python files are embedded into the binary via `include_str!` in src/.
    // Cargo does not track files pulled in by `include_str!`, so without these
    // lines, editing them would NOT trigger a rebuild (the stale text stays baked
    // in). Re-run the build whenever they change.
    println!("cargo:rerun-if-changed=python/pyronova/_bootstrap.py");
    println!("cargo:rerun-if-changed=python/pyronova/_async_engine.py");
}
