//! Error and Result types for GPDB.

use std::sync::Arc;
use thiserror::Error;

/// A type representing errors that can occur during database operations.
#[derive(Error, Debug, Clone)]
pub enum Error {
    /// The error used for I/O operations.
    #[error("IO error: {0}")]
    Io(#[from] Arc<std::io::Error>),

    /// The error used for data corruption.
    #[error("Data corruption: {0}")]
    Corruption(String),

    /// The error used for serialization or deserialization.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// The error used for invalid data.
    #[error("Invalid data: {0}")]
    InvalidData(String),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(Arc::new(err))
    }
}

/// A result type used for GPDB operations.
pub type Result<T> = std::result::Result<T, Error>;
