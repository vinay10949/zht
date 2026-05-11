//! System-wide constants for ZHT.
//!
//! Maps directly to the `Const.h` / `Const-impl.h` / `Env.h` files
//! from the original C++ implementation.

// ── Operation Codes (ZSC_OPC_*) ──────────────────────────────────────
// These are the byte opcodes sent in the ZPack message.

/// Lookup operation code: `"001"`
pub const OPC_LOOKUP: &str = "001";
/// Remove operation code: `"002"`
pub const OPC_REMOVE: &str = "002";
/// Insert operation code: `"003"`
pub const OPC_INSERT: &str = "003";
/// Append operation code: `"004"`
pub const OPC_APPEND: &str = "004";
/// Compare-and-swap operation code: `"005"`
pub const OPC_CMPSWP: &str = "005";
/// State change callback operation code: `"006"`
pub const OPC_STCHGCB: &str = "006";
/// Broadcast global membership done
pub const OPC_BRDDN_GMEM: &str = "007";
/// Cancel an operation
pub const OPC_OPR_CANCEL: &str = "008";
/// Get assigned neighbor info
pub const OPC_GET_ASNGHB: &str = "009";
/// Broadcast global membership
pub const OPC_BRD_GMEM: &str = "010";
/// Get global membership
pub const OPC_GET_GMEM: &str = "011";
/// Get destination ZHT for client request
pub const OPC_GET_DESTZHT: &str = "012";
/// Pull file from source
pub const OPC_PULLFILE: &str = "013";
/// Migrate done (target notification)
pub const OPC_MIGDONETGT: &str = "014";
/// Migrate done (source notification)
pub const OPC_MIGDONESRC: &str = "015";
/// Migrate target
pub const OPC_MIGTARGET: &str = "016";
/// Migrate source
pub const OPC_MIGSOURCE: &str = "017";

// ── Return Codes (ZSC_REC_*) ─────────────────────────────────────────
// These are the status code prefixes returned in responses.

/// Success return code: `"000"`
pub const REC_SUCC: &str = "000";
/// Empty key error: `"-01"`
pub const REC_EMPTYKEY: &str = "-01";
/// Client-side failure: `"-02"`
pub const REC_CLTFAIL: &str = "-02";
/// Server-side failure: `"-03"`
pub const REC_SRVFAIL: &str = "-03";
/// State change callback poll retry: `"-04"`
pub const REC_SCCBPOLLTRY: &str = "-04";
/// Server exception: `"-05"`
pub const REC_SRVEXP: &str = "-05";
/// Non-existent key: `"-92"`
pub const REC_NONEXISTKEY: &str = "-92";
/// No destination ZHT for key: `"-93"`
pub const REC_NODESTZHT: &str = "-93";
/// No need to migrate: `"-94"`
pub const REC_NONEEDMIG: &str = "-94";
/// File push failed: `"-95"`
pub const REC_FLPUSHFAIL: &str = "-95";
/// File pull failed: `"-96"`
pub const REC_FLPULLFAIL: &str = "-96";
/// Second try: `"-97"`
pub const REC_SECDTRY: &str = "-97";
/// Unrecognized operation code: `"-98"`
pub const REC_UOPC: &str = "-98";
/// Unprocessed: `"-99"`
pub const REC_UNPR: &str = "-99";

// ── Protocol Values ──────────────────────────────────────────────────

/// TCP protocol identifier
pub const PROTO_VAL_TCP: &str = "TCP";
/// UDP protocol identifier
pub const PROTO_VAL_UDP: &str = "UDP";
/// MPI protocol identifier
pub const PROTO_VAL_MPI: &str = "MPI";

// ── Environment Defaults ─────────────────────────────────────────────

/// Size of blob transferred from client to server each time (bytes).
/// Corresponds to `Env::BUF_SIZE`.
pub const BUF_SIZE: usize = 550;

/// Default maximum message size per transfer (bytes).
/// Corresponds to `Env::MSG_DEFAULTSIZE`.
pub const MSG_DEFAULT_SIZE: usize = 1024;

/// Default polling interval for state_change_callback (milliseconds).
/// Corresponds to `Env::SCCB_POLL_DEFAULT_INTERVAL`.
pub const SCCB_POLL_DEFAULT_INTERVAL: u64 = 100;

// ── Configuration Parameter Names ─────────────────────────────────────

pub const CONF_PROTOCOL: &str = "PROTOCOL";
pub const CONF_PORT: &str = "PORT";
pub const CONF_MSG_MAXSIZE: &str = "MSG_MAXSIZE";
pub const CONF_SCCB_POLL_INTERVAL: &str = "SCCB_POLL_INTERVAL";
pub const CONF_INSTANT_SWAP: &str = "INSTANT_SWAP";
pub const CONF_MAX_ZHT: &str = "MAX_ZHT";
pub const CONF_NUM_REPLICAS: &str = "NUM_REPLICAS";
pub const CONF_REPLICATION_TYPE: &str = "REPLICATION_TYPE";
pub const CONF_ZHT_CAPACITY: &str = "ZHT_CAPACITY";
pub const CONF_FILECLIENT_PATH: &str = "FILECLIENT_PATH";
pub const CONF_FILESERVER_PATH: &str = "FILESERVER_PATH";
pub const CONF_FILESERVER_PORT: &str = "FILESERVER_PORT";
pub const CONF_HTDATA_PATH: &str = "HTDATA_PATH";
pub const CONF_MIGSLP_TIME: &str = "MIGSLP_TIME";

// ── Misc ─────────────────────────────────────────────────────────────

/// Default hash function base length.
pub const LEN_BASE: usize = 15;

/// Delimiters used in configuration files (space, tab).
pub const CONF_DELIMITERS: &str = " \t";

/// Maximum number of events in the epoll set.
pub const MAX_EVENTS: usize = 4096;
