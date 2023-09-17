use std::fmt;
use std::io;
use std::string;

/// Represents errors that can occur within the `FileManager`.
///
/// This enum contains variants for IO errors, page-related errors, and mutex lock errors.
#[derive(Debug)]
pub enum FileError {
    /// Wrapper around standard IO errors.
    Io(io::Error),

    /// Errors specific to page operations.
    Page(PageError),
}

/// Represents errors that can occur during operations on a `Page`.
///
/// This enum contains variants for IO errors and UTF-8 conversion errors.
#[derive(Debug)]
pub enum PageError {
    /// Wrapper around standard IO errors.
    IoError(io::Error),

    /// Errors that occur during UTF-8 string conversion.
    Utf8Error(std::string::FromUtf8Error),
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileError::Io(err) => write!(f, "IO error: {}", err),
            FileError::Page(err) => write!(f, "Page error: {:?}", err),
        }
    }
}

impl std::error::Error for FileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FileError::Io(err) => Some(err),
            FileError::Page(err) => Some(err),
        }
    }
}

impl fmt::Display for PageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PageError::IoError(err) => write!(f, "IO error: {}", err),
            PageError::Utf8Error(err) => write!(f, "UTF-8 conversion error: {}", err),
        }
    }
}

impl std::error::Error for PageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PageError::IoError(err) => Some(err),
            PageError::Utf8Error(err) => Some(err),
        }
    }
}

impl From<io::Error> for FileError {
    fn from(error: io::Error) -> Self {
        FileError::Io(error)
    }
}

impl From<PageError> for FileError {
    fn from(error: PageError) -> Self {
        FileError::Page(error)
    }
}

impl From<io::Error> for PageError {
    fn from(error: io::Error) -> Self {
        PageError::IoError(error)
    }
}

impl From<string::FromUtf8Error> for PageError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        PageError::Utf8Error(error)
    }
}
