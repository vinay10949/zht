//! Message framing for ZHT wire protocol.
//!
//! All ZHT messages are transmitted with a 4-byte big-endian length prefix
//! followed by the protobuf-encoded `ZPack` payload:
//!
//! ```text
//! [4 bytes: u32 message length (BE)] [N bytes: serialized ZPack protobuf]
//! ```

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use zht_common::error::{ZhtError, ZhtResult};
use zht_proto::ZPack;

/// Encode a `ZPack` message with a 4-byte big-endian length prefix.
///
/// Returns the complete frame ready for network transmission.
pub fn encode_message(zpack: &ZPack) -> Vec<u8> {
    let msg_bytes = zpack.encode_to_vec();
    let len = msg_bytes.len() as u32;
    let mut buf = Vec::with_capacity(4 + msg_bytes.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&msg_bytes);
    buf
}

/// Decode a length-prefixed `ZPack` message from a buffer.
///
/// Returns `(decoded ZPack, remaining bytes)` on success.
pub fn decode_message(buf: &[u8]) -> ZhtResult<(ZPack, &[u8])> {
    if buf.len() < 4 {
        return Err(ZhtError::RecvFailed(
            "Message too short: need at least 4 bytes for length prefix".into(),
        ));
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return Err(ZhtError::RecvFailed(format!(
            "Incomplete message: expected {} bytes, got {}",
            4 + len,
            buf.len()
        )));
    }
    let zpack = ZPack::decode(&buf[4..4 + len])
        .map_err(|e| ZhtError::ProtobufDecodeError(e.to_string()))?;
    Ok((zpack, &buf[4 + len..]))
}

/// Send a `ZPack` message over a TCP stream with length-prefix framing.
pub async fn send_message(stream: &mut TcpStream, zpack: &ZPack) -> ZhtResult<()> {
    let data = encode_message(zpack);
    stream
        .write_all(&data)
        .await
        .map_err(|e| ZhtError::SendFailed(e.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|e| ZhtError::SendFailed(e.to_string()))?;
    Ok(())
}

/// Receive a length-prefixed `ZPack` message from a TCP stream.
pub async fn recv_message(stream: &mut TcpStream) -> ZhtResult<ZPack> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| ZhtError::RecvFailed(e.to_string()))?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut msg_buf = vec![0u8; len];
    stream
        .read_exact(&mut msg_buf)
        .await
        .map_err(|e| ZhtError::RecvFailed(e.to_string()))?;

    let zpack = ZPack::decode(msg_buf.as_slice())
        .map_err(|e| ZhtError::ProtobufDecodeError(e.to_string()))?;
    Ok(zpack)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zht_common::constants::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let mut original = ZPack::default();
        original.opcode = OPC_INSERT.as_bytes().to_vec();
        original.key = b"test_key".to_vec();
        original.val = b"test_value".to_vec();

        let encoded = encode_message(&original);
        assert!(encoded.len() > 4, "Encoded message should have length prefix");

        let len_prefix = u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert_eq!(len_prefix as usize, encoded.len() - 4);

        let (decoded, remaining) = decode_message(&encoded).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(decoded.opcode, original.opcode);
        assert_eq!(decoded.key, original.key);
        assert_eq!(decoded.val, original.val);
    }

    #[test]
    fn test_decode_empty_buffer() {
        let result = decode_message(&[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }

    #[test]
    fn test_decode_too_short_for_length() {
        let buf = [0u8; 3];
        let result = decode_message(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_incomplete_message() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&100u32.to_be_bytes());
        buf.extend_from_slice(&[0u8; 6]);
        let result = decode_message(&buf);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Incomplete"));
    }

    #[test]
    fn test_decode_multiple_messages() {
        let msg1 = {
            let mut z = ZPack::default();
            z.opcode = OPC_INSERT.as_bytes().to_vec();
            z.key = b"key1".to_vec();
            z.val = b"val1".to_vec();
            z
        };
        let msg2 = {
            let mut z = ZPack::default();
            z.opcode = OPC_LOOKUP.as_bytes().to_vec();
            z.key = b"key2".to_vec();
            z
        };

        let mut buf = Vec::new();
        buf.extend(encode_message(&msg1));
        buf.extend(encode_message(&msg2));

        let (decoded1, remaining) = decode_message(&buf).unwrap();
        assert_eq!(decoded1.opcode, msg1.opcode);
        assert_eq!(decoded1.key, msg1.key);

        let (decoded2, remaining) = decode_message(remaining).unwrap();
        assert_eq!(decoded2.opcode, msg2.opcode);
        assert_eq!(decoded2.key, msg2.key);
        assert!(remaining.is_empty());
    }
}
