use crate::file::err::FileManagerError;
use crate::log::err::LogError;
use std::fmt;
use std::sync::MutexGuard;
use std::sync::PoisonError;

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
    FileManagerError(FileManagerError),

    /// An error related to the LogManager.
    LogError(LogError),

    /// An exception indicating a buffer request cannot be satisfied.
    BufferAbortException(BufferAbortException),

    /// An error when a mutex lock fails.
    MutexLockError,
}

impl fmt::Display for BufferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BufferError::FileManagerError(err) => write!(f, "FileManager error: {}", err),
            BufferError::LogError(err) => write!(f, "Log error: {}", err),
            BufferError::BufferAbortException(err) => write!(f, "Buffer abort exception: {}", err),
            BufferError::MutexLockError => write!(f, "Mutex lock error"),
        }
    }
}

impl std::error::Error for BufferError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BufferError::FileManagerError(err) => Some(err),
            BufferError::LogError(err) => Some(err),
            BufferError::BufferAbortException(err) => Some(err),
            BufferError::MutexLockError => None,
        }
    }
}

/// Implement the conversion from FileManagerError to BufferError.
impl From<FileManagerError> for BufferError {
    fn from(error: FileManagerError) -> Self {
        BufferError::FileManagerError(error)
    }
}

/// Implement the conversion from LogError to BufferError.
impl From<LogError> for BufferError {
    fn from(error: LogError) -> Self {
        BufferError::LogError(error)
    }
}

/// Implement the conversion from BufferAbortException to BufferError.
impl From<BufferAbortException> for BufferError {
    fn from(error: BufferAbortException) -> Self {
        BufferError::BufferAbortException(error)
    }
}

/// Implement the conversion from a mutex poisoning error to BufferError.
impl<T: 'static> From<PoisonError<MutexGuard<'_, T>>> for BufferError {
    fn from(_error: PoisonError<MutexGuard<'_, T>>) -> Self {
        BufferError::MutexLockError
    }
}
