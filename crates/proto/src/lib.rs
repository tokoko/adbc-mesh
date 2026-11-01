//! Wire protocol between the untrusted driver shim and the trusted sidecar.
//!
//! Control messages are JSON datagrams on a `SOCK_SEQPACKET` Unix socket. Arrow data never
//! crosses the socket: each batch lives in its own sealed memfd, passed with `SCM_RIGHTS`.

pub mod batch;
pub mod socket;

use serde::{Deserialize, Serialize};

/// Upper bound on a control datagram. Anything larger is a protocol error.
pub const MAX_FRAME: usize = 128 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    /// Start a result stream. Only valid as the first request on a connection.
    Query { sql: String },
    /// Fetch the schema of a table.
    TableSchema { table: String },
    /// Fetch the next batch of the stream.
    Next,
    /// The consumer released its last reference to a batch.
    Release { id: u64 },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    /// Accompanied by a memfd holding the IPC-encoded schema.
    Schema { len: u64 },
    /// Accompanied by a memfd holding the batch.
    Batch { id: u64, len: u64 },
    End,
    Error { kind: ErrorKind, message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorKind {
    /// The request is outside the supported grammar or protocol.
    Invalid,
    /// The consumer holds too much unreleased data, or the sidecar is at capacity.
    Exhausted,
    /// The backend failed. Details stay in the sidecar log.
    Backend,
}
