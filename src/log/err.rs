use crate::file::err::FileError;
use crate::file::err::PageError;
use std::fmt;
use std::io;

/// Represents errors that can occur within the `LogManager`.
///
/// This enum contains variants for FileManager errors, Page errors, standard IO errors,
/// and mutex lock errors.
#[derive(Debug)]
pub enum LogError {
    /// An error related to the File operations.
    FileError(FileError),

    /// An error related to the Page operations.
    PageError(PageError),

    /// Wrapper around standard IO errors.
    IoError(io::Error),
}

impl fmt::Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogError::FileError(err) => write!(f, "FileManager error: {}", err),
            LogError::PageError(err) => write!(f, "Page error: {}", err),
            LogError::IoError(err) => write!(f, "IO error: {}", err),
        }
    }
}

impl std::error::Error for LogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LogError::FileError(err) => Some(err),
            LogError::PageError(err) => Some(err),
            LogError::IoError(err) => Some(err),
        }
    }
}

impl From<FileError> for LogError {
    fn from(error: FileError) -> Self {
        LogError::FileError(error)
    }
}

impl From<PageError> for LogError {
    fn from(error: PageError) -> Self {
        LogError::PageError(error)
    }
}

impl From<io::Error> for LogError {
    fn from(error: io::Error) -> Self {
        LogError::IoError(error)
    }
}
