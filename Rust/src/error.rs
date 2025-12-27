//! Error types for StreamDB.

use std::fmt;
use std::io;

/// Result type alias for StreamDB operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for StreamDB operations
#[derive(Debug)]
pub enum Error {
    /// I/O error from file operations
    Io(io::Error),
    
    /// Key or document not found
    NotFound(String),
    
    /// Invalid input (empty key, key too long, etc.)
    InvalidInput(String),
    
    /// Data corruption detected (checksum mismatch, invalid format)
    Corrupted(String),
    
    /// Serialization/deserialization error
    Serialization(String),
    
    /// Database is full or resource limit reached
    ResourceLimit(String),
    
    /// Transaction error
    Transaction(String),
    
    /// Encryption/decryption error
    #[cfg(feature = "encryption")]
    Encryption(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {}", e),
            Error::NotFound(msg) => write!(f, "Not found: {}", msg),
            Error::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            Error::Corrupted(msg) => write!(f, "Data corrupted: {}", msg),
            Error::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            Error::ResourceLimit(msg) => write!(f, "Resource limit: {}", msg),
            Error::Transaction(msg) => write!(f, "Transaction error: {}", msg),
            #[cfg(feature = "encryption")]
            Error::Encryption(msg) => write!(f, "Encryption error: {}", msg),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Error::Serialization(e.to_string())
    }
}

impl From<uuid::Error> for Error {
    fn from(e: uuid::Error) -> Self {
        Error::InvalidInput(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_error_display() {
        let err = Error::NotFound("test key".into());
        assert!(err.to_string().contains("Not found"));
        assert!(err.to_string().contains("test key"));
    }
    
    #[test]
    fn test_io_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }
}
