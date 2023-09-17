use crate::buffer::err::BufferError;
use crate::file::err::FileError;
use crate::file::err::PageError;
use crate::transaction::concurrency::err::ConcurrencyError;
use crate::transaction::recovery::err::RecoveryError;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum TransactionError {
    BufferError(BufferError),
    ConcurrencyError(ConcurrencyError),
    RecoveryError(RecoveryError),
    PageError(PageError),
    FileError(FileError),
    BufferNotFoundError,
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TransactionError::BufferError(err) => write!(f, "BufferError: {}", err),
            TransactionError::ConcurrencyError(err) => write!(f, "ConcurrencyError: {}", err),
            TransactionError::RecoveryError(err) => write!(f, "RecoveryError: {}", err),
            TransactionError::PageError(err) => write!(f, "PageError: {}", err),
            TransactionError::FileError(err) => write!(f, "FileError: {}", err),
            TransactionError::BufferNotFoundError => write!(f, "Buffer not found"),
        }
    }
}

impl Error for TransactionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TransactionError::BufferError(err) => Some(err),
            TransactionError::ConcurrencyError(err) => Some(err),
            TransactionError::RecoveryError(err) => Some(err),
            TransactionError::PageError(err) => Some(err),
            TransactionError::FileError(err) => Some(err),
            TransactionError::BufferNotFoundError => None,
        }
    }
}

impl From<BufferError> for TransactionError {
    fn from(err: BufferError) -> TransactionError {
        TransactionError::BufferError(err)
    }
}

impl From<ConcurrencyError> for TransactionError {
    fn from(err: ConcurrencyError) -> TransactionError {
        TransactionError::ConcurrencyError(err)
    }
}

impl From<RecoveryError> for TransactionError {
    fn from(err: RecoveryError) -> TransactionError {
        TransactionError::RecoveryError(err)
    }
}

impl From<PageError> for TransactionError {
    fn from(err: PageError) -> TransactionError {
        TransactionError::PageError(err)
    }
}

impl From<FileError> for TransactionError {
    fn from(err: FileError) -> TransactionError {
        TransactionError::FileError(err)
    }
}
