fn main() {
    let proto_files = &["../../proto/zpack.proto", "../../proto/meta.proto"];

    // Tell Cargo to re-run if proto files change
    for proto in proto_files {
        println!("cargo:rerun-if-changed={}", proto);
    }

    // Specify the protoc compiler path
    let protoc_path = "/home/z/.local/bin/protoc";

    prost_build::Config::new()
        .compile_protos(proto_files, &["../../proto/"])
        .expect("Failed to compile protobuf files");

    // Rerun if protoc binary changes (optional, for CI reproducibility)
    println!("cargo:rerun-if-changed={}", protoc_path);
}
