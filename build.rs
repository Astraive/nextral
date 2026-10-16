fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(&["contracts/grpc/nextral.proto"], &["contracts/grpc"])?;
    println!("cargo:rerun-if-changed=contracts/grpc/nextral.proto");
    println!("cargo:rerun-if-changed=contracts/graphql/schema.graphql");
    Ok(())
}
