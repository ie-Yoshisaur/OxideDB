use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum ConcurrencyError {
    LockAbortError,
}

#[derive(Debug)]
pub enum LockAbortError {
    Timeout,
}

impl fmt::Display for ConcurrencyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConcurrencyError::LockAbortError => {
                write!(f, "Failed to acquire X lock within the time limit")
            }
        }
    }
}

impl Error for ConcurrencyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ConcurrencyError::LockAbortError => None,
        }
    }
}
