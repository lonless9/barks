fn main() -> Result<(), Box<dyn std::error::Error>> {
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
