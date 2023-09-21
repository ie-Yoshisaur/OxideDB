use crate::buffer::err::BufferError;
use crate::file::err::PageError;
use crate::log::err::LogError;
use std::error::Error;
use std::fmt;

// `LogRecordError` enum represents errors that can occur related to a log record.
#[derive(Debug)]
pub enum LogRecordError {
    PageError(PageError),
    LogError(LogError),
}

impl fmt::Display for LogRecordError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LogRecordError::PageError(err) => write!(f, "PageError: {}", err),
            LogRecordError::LogError(err) => write!(f, "LogError: {}", err),
        }
    }
}

impl Error for LogRecordError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            LogRecordError::PageError(err) => Some(err),
            LogRecordError::LogError(err) => Some(err),
        }
    }
}

impl From<PageError> for LogRecordError {
    fn from(err: PageError) -> LogRecordError {
        LogRecordError::PageError(err)
    }
}

impl From<LogError> for LogRecordError {
    fn from(err: LogError) -> LogRecordError {
        LogRecordError::LogError(err)
    }
}

// `RecoveryError` enum represents errors that can occur during recovery.
#[derive(Debug)]
pub enum RecoveryError {
    LogRecordError(LogRecordError),
    BufferError(BufferError),
    LogError(LogError),
    PageError(PageError),
    BlockNotFoundError,
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RecoveryError::LogRecordError(err) => write!(f, "LogRecordError: {}", err),
            RecoveryError::BufferError(err) => write!(f, "BufferError: {}", err),
            RecoveryError::LogError(err) => write!(f, "LogError: {}", err),
            RecoveryError::PageError(err) => write!(f, "PageError: {}", err),
            RecoveryError::BlockNotFoundError => write!(f, "Block not found"),
        }
    }
}

impl Error for RecoveryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            RecoveryError::LogRecordError(err) => Some(err),
            RecoveryError::BufferError(err) => Some(err),
            RecoveryError::LogError(err) => Some(err),
            RecoveryError::PageError(err) => Some(err),
            RecoveryError::BlockNotFoundError => None,
        }
    }
}

impl From<LogRecordError> for RecoveryError {
    fn from(err: LogRecordError) -> RecoveryError {
        RecoveryError::LogRecordError(err)
    }
}

impl From<BufferError> for RecoveryError {
    fn from(err: BufferError) -> RecoveryError {
        RecoveryError::BufferError(err)
    }
}

impl From<LogError> for RecoveryError {
    fn from(err: LogError) -> RecoveryError {
        RecoveryError::LogError(err)
    }
}

impl From<PageError> for RecoveryError {
    fn from(err: PageError) -> RecoveryError {
        RecoveryError::PageError(err)
    }
}
