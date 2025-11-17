use std::path::Path;

fn main() -> Result<(), String> {
    let proto_path = Path::new("proto/datafusion_loki.proto");
    let out_dir = Path::new(env!("OUT_DIR"));

    prost_build::Config::new()
        .out_dir(out_dir)
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(".datafusion_common", "::datafusion_proto::protobuf")
        .extern_path(".datafusion", "::datafusion_proto::protobuf")
        .compile_well_known_types()
        .compile_protos(&[proto_path], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {e}"))?;

    let prost = out_dir.join("datafusion_loki.rs");
    let target = Path::new("../src/protobuf.rs");
    println!("Copying {} to {}", prost.display(), target.display(),);
    std::fs::copy(prost, target).unwrap();

    Ok(())
}
