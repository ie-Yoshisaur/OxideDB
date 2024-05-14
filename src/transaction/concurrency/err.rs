use std::error::Error;
use std::fmt;

// `ConcurrencyError` enum represents errors related to concurrency.
#[derive(Debug)]
pub enum ConcurrencyError {
    LockAbortError,
    Timeout,
}

impl fmt::Display for ConcurrencyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConcurrencyError::LockAbortError => {
                write!(f, "Failed to acquire X lock within the time limit")
            }
            ConcurrencyError::Timeout => {
                write!(f, "Failed to acquire S lock within the time limit")
            }
        }
    }
}

// `LockAbortError` enum represents errors related to lock abortion.
#[derive(Debug)]
pub enum LockAbortError {
    Timeout,
}

impl Error for ConcurrencyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ConcurrencyError::LockAbortError => None,
            ConcurrencyError::Timeout => None,
        }
    }
}
