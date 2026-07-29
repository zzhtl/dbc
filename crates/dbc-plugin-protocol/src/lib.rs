//! Versioned process-driver protocol and bounded frame codec.

use bytes::Bytes;
use prost::Message;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub mod generated;

use generated::dbc::driver::v1::{FrameHeader, FrameKind};

pub const CURRENT_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::new(1, 1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolVersion {
    pub major: u32,
    pub minor: u32,
}

impl ProtocolVersion {
    #[must_use]
    pub const fn new(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }

    #[must_use]
    pub const fn accepts(self, plugin: Self) -> bool {
        self.major == plugin.major && plugin.minor <= self.minor
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecLimits {
    pub max_header_bytes: usize,
    pub max_payload_bytes: usize,
}

impl Default for CodecLimits {
    fn default() -> Self {
        Self {
            max_header_bytes: 64 * 1024,
            max_payload_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub header: FrameHeader,
    pub payload: Bytes,
}

impl Frame {
    #[must_use]
    pub fn new(
        request_id: impl Into<String>,
        kind: FrameKind,
        sequence: u64,
        end_stream: bool,
        payload: Bytes,
    ) -> Self {
        Self {
            header: FrameHeader {
                request_id: request_id.into(),
                kind: kind as i32,
                sequence,
                end_stream,
                payload_length: payload.len() as u64,
            },
            payload,
        }
    }
}

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid protobuf header: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("header length {actual} exceeds limit {limit}")]
    HeaderTooLarge { actual: usize, limit: usize },
    #[error("payload length {actual} exceeds limit {limit}")]
    PayloadTooLarge { actual: usize, limit: usize },
    #[error("payload length cannot be represented on this platform")]
    PayloadLengthOverflow,
}

#[derive(Debug, Clone, Copy)]
pub struct FrameCodec {
    limits: CodecLimits,
}

impl FrameCodec {
    #[must_use]
    pub fn new(limits: CodecLimits) -> Self {
        Self { limits }
    }

    /// Write one length-delimited header and its raw payload.
    ///
    /// # Errors
    ///
    /// Rejects frames above configured limits and propagates I/O errors.
    pub async fn write_frame<W>(&self, writer: &mut W, frame: &Frame) -> Result<(), CodecError>
    where
        W: AsyncWrite + Unpin,
    {
        let payload_len = frame.payload.len();
        if payload_len > self.limits.max_payload_bytes {
            return Err(CodecError::PayloadTooLarge {
                actual: payload_len,
                limit: self.limits.max_payload_bytes,
            });
        }

        let mut header = frame.header.clone();
        header.payload_length =
            u64::try_from(payload_len).map_err(|_| CodecError::PayloadLengthOverflow)?;
        let header_len = header.encoded_len();
        if header_len > self.limits.max_header_bytes {
            return Err(CodecError::HeaderTooLarge {
                actual: header_len,
                limit: self.limits.max_header_bytes,
            });
        }
        let header_len =
            u32::try_from(header_len).map_err(|_| CodecError::HeaderTooLarge {
                actual: header.encoded_len(),
                limit: u32::MAX as usize,
            })?;
        let mut encoded = Vec::with_capacity(header_len as usize);
        header
            .encode(&mut encoded)
            .map_err(|error| CodecError::Io(std::io::Error::other(error)))?;

        writer.write_u32(header_len).await?;
        writer.write_all(&encoded).await?;
        writer.write_all(&frame.payload).await?;
        writer.flush().await?;
        Ok(())
    }

    /// Read one bounded frame.
    ///
    /// # Errors
    ///
    /// Rejects oversized or malformed headers and payloads before allocating them.
    pub async fn read_frame<R>(&self, reader: &mut R) -> Result<Frame, CodecError>
    where
        R: AsyncRead + Unpin,
    {
        let header_len = reader.read_u32().await? as usize;
        if header_len > self.limits.max_header_bytes {
            return Err(CodecError::HeaderTooLarge {
                actual: header_len,
                limit: self.limits.max_header_bytes,
            });
        }

        let mut header_bytes = vec![0; header_len];
        reader.read_exact(&mut header_bytes).await?;
        let header = FrameHeader::decode(header_bytes.as_slice())?;
        let payload_len = usize::try_from(header.payload_length)
            .map_err(|_| CodecError::PayloadLengthOverflow)?;
        if payload_len > self.limits.max_payload_bytes {
            return Err(CodecError::PayloadTooLarge {
                actual: payload_len,
                limit: self.limits.max_payload_bytes,
            });
        }

        let mut payload = vec![0; payload_len];
        reader.read_exact(&mut payload).await?;
        Ok(Frame {
            header,
            payload: Bytes::from(payload),
        })
    }
}
