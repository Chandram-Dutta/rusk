use std::io;

pub type Result<T> = std::result::Result<T, RuskError>;

#[derive(Debug)]
pub enum RuskError {
    /// IO error during file operations
    Io(io::Error),
    /// Serialization/deserialization error
    Serde(serde_json::Error),
    /// Key not found in the store
    KeyNotFound,
    /// Unexpected command type during read
    UnexpectedCommand,
}

impl std::fmt::Display for RuskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuskError::Io(err) => write!(f, "IO error: {}", err),
            RuskError::Serde(err) => write!(f, "Serialization error: {}", err),
            RuskError::KeyNotFound => write!(f, "Key not found"),
            RuskError::UnexpectedCommand => write!(f, "Unexpected command"),
        }
    }
}

impl std::error::Error for RuskError {}

impl From<io::Error> for RuskError {
    fn from(err: io::Error) -> Self {
        RuskError::Io(err)
    }
}

impl From<serde_json::Error> for RuskError {
    fn from(err: serde_json::Error) -> Self {
        RuskError::Serde(err)
    }
}
