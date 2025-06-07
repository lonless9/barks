fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../proto/driver.proto");
    println!("cargo:rerun-if-changed=../proto/executor.proto");

    // Generate Rust code from proto files using latest tonic-build API
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["../proto/driver.proto", "../proto/executor.proto"],
            &["../proto"],
        )?;

    Ok(())
}
