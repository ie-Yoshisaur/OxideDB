use std::fmt;

/// Represents errors that can occur within the logging system.
///
/// This enum contains variants for file manager errors, block-related errors,
/// page-related errors, IO errors, and mutex lock errors.
#[derive(Debug)]
pub enum LogError {
    /// Errors specific to file manager operations.
    FileManagerError(String),

    /// Errors specific to block operations.
    BlockError(String),

    /// Errors specific to page operations.
    PageError(String),

    /// Wrapper around standard IO errors.
    IOError(String),

    /// An error indicating that locking a mutex failed.
    MutexLockError,
}

impl fmt::Display for LogError {
    /// Formats the `LogError` variants as a human-readable string.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LogError::FileManagerError(msg) => write!(f, "FileManagerError: {}", msg),
            LogError::BlockError(msg) => write!(f, "BlockError: {}", msg),
            LogError::PageError(msg) => write!(f, "PageError: {}", msg),
            LogError::IOError(msg) => write!(f, "IOError: {}", msg),
            LogError::MutexLockError => write!(f, "MutexLockError: Failed to acquire lock"),
        }
    }
}

impl std::error::Error for LogError {
    /// Provides a source error, if any, for the `LogError` variants.
    ///
    /// Currently, this function returns `None` as the `LogError` variants
    /// do not wrap other errors.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}
