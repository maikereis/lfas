/// Python stub generator (.pyi) for the LFAS module.
///
/// Run with:
/// cargo run --bin stub_gen
///
/// This automatically generates `python/lfas/lfas.pyi` from
/// the Rust documentation comments in src/python.rs.
fn main() {
    // Points to the stub_info() function exposed in python.rs
    let stub = lfas::python::stub_info();
    stub.generate().expect("Failed to generate .pyi file.");
    println!("Stub generated successfully!");
}