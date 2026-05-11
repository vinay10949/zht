//! HTWorker — Server-side request processor.
//!
//! Dispatches ZHT operations from incoming `ZPack` requests to the
//! `NoVoHT` storage engine.

use zht_common::constants::*;
use zht_proto::ZPack;
use zht_storage::NoVoHT;

/// Server-side request processor that dispatches ZHT operations to the
/// `NoVoHT` storage engine.
///
/// `HTWorker` receives a `ZPack` request, determines the operation from the
/// opcode field, executes the corresponding storage operation, and returns
/// a response `ZPack`.
///
/// # Supported Operations
///
/// | Opcode | Name                   | Description                                    |
/// |--------|------------------------|------------------------------------------------|
/// | `001`  | LOOKUP                 | Retrieve value by key                           |
/// | `002`  | REMOVE                 | Delete a key-value pair                         |
/// | `003`  | INSERT                 | Create or overwrite a key-value pair            |
/// | `004`  | APPEND                 | Append data to an existing key's value          |
/// | `005`  | COMPARE_SWAP           | Atomic compare-and-swap                         |
/// | `006`  | STATE_CHANGE_CALLBACK  | Block until key matches expected value or lease expires |
pub struct HTWorker {
    /// The underlying NoVoHT storage engine.
    store: NoVoHT,
}

impl HTWorker {
    /// Create a new `HTWorker` with the given storage engine.
    pub fn new(store: NoVoHT) -> Self {
        Self { store }
    }

    /// Get a reference to the underlying storage engine.
    pub fn store(&self) -> &NoVoHT {
        &self.store
    }

    /// Process a single `ZPack` request and return a response `ZPack`.
    ///
    /// This is a synchronous operation that operates on the in-memory
    /// `NoVoHT` hash table. It extracts the opcode, key, and value from
    /// the request, performs the appropriate storage operation, and
    /// populates the response with the result.
    ///
    /// # Response Fields
    /// - `opcode`: Echoed from the request.
    /// - `key`: Echoed from the request.
    /// - `val`: Populated for LOOKUP responses (the retrieved value) and
    ///   COMPARE_SWAP mismatches (the current value).
    /// - `lease`: Encoded status code:
    ///   - `"000..."` = success (LOOKUP appends the value after the status)
    ///   - `"-01"` = empty key error
    ///   - `"-02"` = CAS mismatch
    ///   - `"-03"` = server-side failure
    ///   - `"-04"` = state change callback lease expired
    ///   - `"-92"` = non-existent key
    ///   - `"-98"` = unrecognized opcode
    pub fn process(&self, request: &ZPack) -> ZPack {
        let opcode = String::from_utf8_lossy(&request.opcode).to_string();
        let key = String::from_utf8_lossy(&request.key).to_string();

        let mut response = ZPack::default();
        response.opcode = request.opcode.clone();
        response.key = request.key.clone();

        // Validate key is not empty
        if key.is_empty() {
            response.lease = REC_EMPTYKEY.as_bytes().to_vec();
            return response;
        }

        match opcode.as_str() {
            OPC_LOOKUP => {
                match self.store.get(&key) {
                    Ok(val) => {
                        response.val = val.clone().into_bytes();
                        response.valnull = false;
                        // LOOKUP success format: "000" + value
                        let mut status = REC_SUCC.to_string();
                        status.push_str(&val);
                        response.lease = status.into_bytes();
                    }
                    Err(_) => {
                        response.valnull = true;
                        response.lease = REC_NONEXISTKEY.as_bytes().to_vec();
                    }
                }
            }

            OPC_INSERT => {
                let val = String::from_utf8_lossy(&request.val).to_string();
                match self.store.put(&key, &val) {
                    Ok(()) => {
                        response.lease = REC_SUCC.as_bytes().to_vec();
                    }
                    Err(e) => {
                        response.lease = REC_SRVFAIL.as_bytes().to_vec();
                        tracing::error!("INSERT failed for key '{}': {}", key, e);
                    }
                }
            }

            OPC_REMOVE => {
                match self.store.remove(&key) {
                    Ok(()) => {
                        response.lease = REC_SUCC.as_bytes().to_vec();
                    }
                    Err(_) => {
                        response.lease = REC_NONEXISTKEY.as_bytes().to_vec();
                    }
                }
            }

            OPC_APPEND => {
                let val = String::from_utf8_lossy(&request.val).to_string();
                match self.store.append(&key, &val) {
                    Ok(()) => {
                        response.lease = REC_SUCC.as_bytes().to_vec();
                    }
                    Err(e) => {
                        response.lease = REC_SRVFAIL.as_bytes().to_vec();
                        tracing::error!("APPEND failed for key '{}': {}", key, e);
                    }
                }
            }

            OPC_CMPSWP => {
                let expected = String::from_utf8_lossy(&request.val).to_string();
                let new_val = String::from_utf8_lossy(&request.newval).to_string();
                match self.store.compare_swap(&key, &expected, &new_val) {
                    Ok(actual) => {
                        if actual == new_val {
                            // CAS succeeded
                            response.lease = REC_SUCC.as_bytes().to_vec();
                        } else {
                            // CAS mismatch — return actual value to client
                            response.val = actual.clone().into_bytes();
                            response.valnull = false;
                            response.lease = REC_CLTFAIL.as_bytes().to_vec();
                        }
                    }
                    Err(_) => {
                        // Key not found
                        response.lease = REC_NONEXISTKEY.as_bytes().to_vec();
                    }
                }
            }

            OPC_STCHGCB => {
                // State change callback: poll until value matches expected or lease expires.
                let expected = String::from_utf8_lossy(&request.val).to_string();
                let lease_ms: u64 = String::from_utf8_lossy(&request.lease)
                    .parse()
                    .unwrap_or(SCCB_POLL_DEFAULT_INTERVAL);

                let poll_interval = std::time::Duration::from_millis(SCCB_POLL_DEFAULT_INTERVAL);
                let deadline =
                    tokio::time::Instant::now() + std::time::Duration::from_millis(lease_ms);

                let store_result = tokio::task::block_in_place(|| {
                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(async {
                        loop {
                            match self.store.get(&key) {
                                Ok(ref current) if *current == expected => {
                                    return REC_SUCC.as_bytes().to_vec();
                                }
                                _ => {
                                    if tokio::time::Instant::now() >= deadline {
                                        return REC_SCCBPOLLTRY.as_bytes().to_vec();
                                    }
                                    tokio::time::sleep(poll_interval).await;
                                }
                            }
                        }
                    })
                });

                response.lease = store_result;
            }

            _ => {
                response.lease = REC_UOPC.as_bytes().to_vec();
                tracing::warn!("Unrecognized opcode: {}", opcode);
            }
        }

        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_worker() -> HTWorker {
        let store = NoVoHT::new();
        HTWorker::new(store)
    }

    fn make_zpack(opcode: &str, key: &str, val: &str) -> ZPack {
        let mut zpack = ZPack::default();
        zpack.opcode = opcode.as_bytes().to_vec();
        zpack.key = key.as_bytes().to_vec();
        if !val.is_empty() {
            zpack.val = val.as_bytes().to_vec();
        }
        zpack
    }

    #[test]
    fn test_worker_insert_and_lookup() {
        let worker = make_test_worker();

        let req = make_zpack(OPC_INSERT, "mykey", "myvalue");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());

        let req = make_zpack(OPC_LOOKUP, "mykey", "");
        let resp = worker.process(&req);
        let lease_status = String::from_utf8_lossy(&resp.lease);
        assert!(lease_status.starts_with(REC_SUCC));
        assert!(lease_status.contains("myvalue"));
        assert_eq!(resp.val, b"myvalue".to_vec());
        assert!(!resp.valnull);
    }

    #[test]
    fn test_worker_lookup_nonexistent() {
        let worker = make_test_worker();
        let req = make_zpack(OPC_LOOKUP, "nonexistent", "");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_NONEXISTKEY.as_bytes().to_vec());
        assert!(resp.valnull);
    }

    #[test]
    fn test_worker_remove() {
        let worker = make_test_worker();
        let req = make_zpack(OPC_INSERT, "rmkey", "rmval");
        worker.process(&req);

        let req = make_zpack(OPC_REMOVE, "rmkey", "");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());

        let req = make_zpack(OPC_LOOKUP, "rmkey", "");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_NONEXISTKEY.as_bytes().to_vec());
    }

    #[test]
    fn test_worker_remove_nonexistent() {
        let worker = make_test_worker();
        let req = make_zpack(OPC_REMOVE, "nope", "");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_NONEXISTKEY.as_bytes().to_vec());
    }

    #[test]
    fn test_worker_append() {
        let worker = make_test_worker();
        let req = make_zpack(OPC_INSERT, "appendkey", "hello");
        worker.process(&req);

        let req = make_zpack(OPC_APPEND, "appendkey", " world");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());

        let req = make_zpack(OPC_LOOKUP, "appendkey", "");
        let resp = worker.process(&req);
        assert_eq!(resp.val, b"hello: world".to_vec());
    }

    #[test]
    fn test_worker_compare_swap_success() {
        let worker = make_test_worker();
        let req = make_zpack(OPC_INSERT, "caskey", "old");
        worker.process(&req);

        let mut req = make_zpack(OPC_CMPSWP, "caskey", "old");
        req.newval = b"new".to_vec();
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_SUCC.as_bytes().to_vec());

        let req = make_zpack(OPC_LOOKUP, "caskey", "");
        let resp = worker.process(&req);
        assert_eq!(resp.val, b"new".to_vec());
    }

    #[test]
    fn test_worker_compare_swap_mismatch() {
        let worker = make_test_worker();
        let req = make_zpack(OPC_INSERT, "caskey", "actual");
        worker.process(&req);

        let mut req = make_zpack(OPC_CMPSWP, "caskey", "wrong");
        req.newval = b"new".to_vec();
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_CLTFAIL.as_bytes().to_vec());
        assert_eq!(resp.val, b"actual".to_vec());
        assert!(!resp.valnull);

        let req = make_zpack(OPC_LOOKUP, "caskey", "");
        let resp = worker.process(&req);
        assert_eq!(resp.val, b"actual".to_vec());
    }

    #[test]
    fn test_worker_unrecognized_opcode() {
        let worker = make_test_worker();
        let req = make_zpack("999", "key", "val");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_UOPC.as_bytes().to_vec());
    }

    #[test]
    fn test_worker_empty_key() {
        let worker = make_test_worker();
        let req = make_zpack(OPC_INSERT, "", "val");
        let resp = worker.process(&req);
        assert_eq!(resp.lease, REC_EMPTYKEY.as_bytes().to_vec());
    }
}
