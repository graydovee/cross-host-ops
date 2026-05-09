fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure().compile_protos(&["proto/rhop.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/rhop.proto");
    Ok(())
}
