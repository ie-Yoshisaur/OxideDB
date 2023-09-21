use crate::transaction::err::TransactionError;
use std::fmt;

/// `LayoutError` enum represents errors that can occur related to the `Layout` struct.
#[derive(Debug)]
pub enum LayoutError {
    /// This variant is used when a specified field is not found in the layout.
    FieldNotFoundError,
}

impl fmt::Display for LayoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LayoutError::FieldNotFoundError => write!(f, "Field not found"),
        }
    }
}

impl std::error::Error for LayoutError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

/// `RecordPageError` enum represents errors that can occur related to the `RecordPage` struct.
#[derive(Debug)]
pub enum RecordPageError {
    /// This variant is used when an offset is not found in the record page.
    OffsetNotFoundError,
    /// This variant wraps a `TransactionError` to represent transaction-related errors.
    TransactionError(TransactionError),
    /// This variant is used when a block is not found in the buffer.
    BufferNotFoundError,
    /// This variant is used when a specified field is not found in the record page.
    FieldNotFoundError,
}

impl fmt::Display for RecordPageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecordPageError::OffsetNotFoundError => write!(f, "Offset not found"),
            RecordPageError::TransactionError(err) => write!(f, "Transaction error: {}", err),
            RecordPageError::BufferNotFoundError => write!(f, "Block not found in buffer"),
            RecordPageError::FieldNotFoundError => write!(f, "Field not found"),
        }
    }
}

impl std::error::Error for RecordPageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RecordPageError::TransactionError(err) => Some(err),
            _ => None,
        }
    }
}

/// `TableScanError` enum represents errors that can occur related to the `TableScan` struct.
#[derive(Debug)]
pub enum TableScanError {
    /// This variant wraps a `RecordPageError` to represent errors related to the record page.
    RecordPageError(RecordPageError),
    /// This variant wraps a `TransactionError` to represent transaction-related errors.
    TransactionError(TransactionError),
}

impl fmt::Display for TableScanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableScanError::RecordPageError(err) => write!(f, "Record page error: {}", err),
            TableScanError::TransactionError(err) => write!(f, "Transaction error: {}", err),
        }
    }
}

impl std::error::Error for TableScanError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TableScanError::RecordPageError(err) => Some(err),
            TableScanError::TransactionError(err) => Some(err),
        }
    }
}
