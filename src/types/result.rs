use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] Arc<std::io::Error>),

    #[error("Data corruption: {0}")]
    Corruption(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(Arc::new(err))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
