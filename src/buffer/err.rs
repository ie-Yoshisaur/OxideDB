use crate::file::err::FileError;
use crate::log::err::LogError;
use std::fmt;

/// Represents an exception when a buffer request cannot be fulfilled.
/// This usually means the transaction needs to be aborted.
#[derive(Debug, Clone)]
pub struct BufferAbortException;

impl std::fmt::Display for BufferAbortException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Buffer request could not be satisfied, transaction needs to abort"
        )
    }
}

impl std::error::Error for BufferAbortException {}

/// Enum for Buffer related errors.
///
/// It encapsulates errors from file management, logging, buffer operations,
/// and mutex lock issues.
#[derive(Debug)]
pub enum BufferError {
    /// An error related to the FileManager.
    FileError(FileError),

    /// An error related to the LogManager.
    LogError(LogError),

    /// An exception indicating a buffer request cannot be satisfied.
    BufferAbortException(BufferAbortException),
}

impl fmt::Display for BufferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BufferError::FileError(err) => write!(f, "FileManager error: {}", err),
            BufferError::LogError(err) => write!(f, "Log error: {}", err),
            BufferError::BufferAbortException(err) => write!(f, "Buffer abort exception: {}", err),
        }
    }
}

impl std::error::Error for BufferError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BufferError::FileError(err) => Some(err),
            BufferError::LogError(err) => Some(err),
            BufferError::BufferAbortException(err) => Some(err),
        }
    }
}

impl From<FileError> for BufferError {
    fn from(error: FileError) -> Self {
        BufferError::FileError(error)
    }
}

impl From<LogError> for BufferError {
    fn from(error: LogError) -> Self {
        BufferError::LogError(error)
    }
}

impl From<BufferAbortException> for BufferError {
    fn from(error: BufferAbortException) -> Self {
        BufferError::BufferAbortException(error)
    }
}
