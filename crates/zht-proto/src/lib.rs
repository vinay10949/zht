// ZHT Protocol Buffer Definitions
//
// This module re-exports all generated protobuf types.

/// Include the generated protobuf code
pub mod zht {
    pub mod proto {
        include!(concat!(env!("OUT_DIR"), "/zht.proto.rs"));
    }
}

// Re-export commonly used types at the crate root
pub use zht::proto::{ZPack, Package};
