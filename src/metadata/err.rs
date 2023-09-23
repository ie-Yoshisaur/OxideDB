use crate::record::err::LayoutError;
use crate::record::err::TableScanError;
use std::fmt;

/// Represents errors that can occur within `TableManager`.
#[derive(Debug)]
pub enum TableManagerError {
    /// Error related to table layout.
    LayoutError(LayoutError),
    /// Error occurring during table scan.
    TableScanError(TableScanError),
    /// Error for a field that was not found.
    FieldNotFoundError,
    /// Error for invalid integer values.
    InvalidIntergerError,
}

impl fmt::Display for TableManagerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TableManagerError::LayoutError(err) => write!(f, "TableManagerError error: {}", err),
            TableManagerError::TableScanError(err) => write!(f, "TableManagerError error: {}", err),
            TableManagerError::FieldNotFoundError => write!(f, "Field not found"),
            TableManagerError::InvalidIntergerError => write!(f, "Invalid integer value"),
        }
    }
}

impl std::error::Error for TableManagerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TableManagerError::LayoutError(err) => Some(err),
            TableManagerError::TableScanError(err) => Some(err),
            TableManagerError::FieldNotFoundError => None,
            TableManagerError::InvalidIntergerError => None,
        }
    }
}

impl From<LayoutError> for TableManagerError {
    fn from(error: LayoutError) -> Self {
        TableManagerError::LayoutError(error)
    }
}

impl From<TableScanError> for TableManagerError {
    fn from(error: TableScanError) -> Self {
        TableManagerError::TableScanError(error)
    }
}

/// Represents errors that can occur within `ViewManager`.
#[derive(Debug)]
pub enum ViewManagerError {
    /// Error related to operations in `TableManager`.
    TableManagerError(TableManagerError),
    /// Error occurring during table scan.
    TableScanError(TableScanError),
}

impl fmt::Display for ViewManagerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ViewManagerError::TableManagerError(err) => {
                write!(f, "ViewManagerError error: {}", err)
            }
            ViewManagerError::TableScanError(err) => {
                write!(f, "ViewManagerError error: {}", err)
            }
        }
    }
}

impl std::error::Error for ViewManagerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ViewManagerError::TableManagerError(err) => Some(err),
            ViewManagerError::TableScanError(err) => Some(err),
        }
    }
}

impl From<TableManagerError> for ViewManagerError {
    fn from(error: TableManagerError) -> Self {
        ViewManagerError::TableManagerError(error)
    }
}

impl From<TableScanError> for ViewManagerError {
    fn from(error: TableScanError) -> Self {
        ViewManagerError::TableScanError(error)
    }
}

/// Represents errors that can occur within `StatisticsManager`.
#[derive(Debug)]
pub enum StatisticsManagerError {
    /// Error related to operations in `TableManager`.
    TableManagerError(TableManagerError),
    /// Error occurring during table scan.
    TableScanError(TableScanError),
}

impl fmt::Display for StatisticsManagerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatisticsManagerError::TableManagerError(err) => {
                write!(f, "StatisticsManagerError error: {}", err)
            }
            StatisticsManagerError::TableScanError(err) => {
                write!(f, "StatisticsManagerError error: {}", err)
            }
        }
    }
}

impl std::error::Error for StatisticsManagerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StatisticsManagerError::TableManagerError(err) => Some(err),
            StatisticsManagerError::TableScanError(err) => Some(err),
        }
    }
}

impl From<TableManagerError> for StatisticsManagerError {
    fn from(error: TableManagerError) -> Self {
        StatisticsManagerError::TableManagerError(error)
    }
}

impl From<TableScanError> for StatisticsManagerError {
    fn from(error: TableScanError) -> Self {
        StatisticsManagerError::TableScanError(error)
    }
}

/// Represents a comprehensive error type that can occur within `MetadataManager`.
#[derive(Debug)]
pub enum MetadataManagerError {
    /// Error related to operations in `TableManager`.
    TableManagerError(TableManagerError),
    /// Error related to operations in `ViewManager`.
    ViewManagerError(ViewManagerError),
    /// Error related to operations in `StatisticsManager`.
    StatisticsManagerError(StatisticsManagerError),
}

impl fmt::Display for MetadataManagerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetadataManagerError::TableManagerError(err) => {
                write!(f, "MetadataManagerError error: {}", err)
            }
            MetadataManagerError::ViewManagerError(err) => {
                write!(f, "MetadataManagerError error: {}", err)
            }
            MetadataManagerError::StatisticsManagerError(err) => {
                write!(f, "MetadataManagerError error: {}", err)
            }
        }
    }
}

impl std::error::Error for MetadataManagerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MetadataManagerError::TableManagerError(err) => Some(err),
            MetadataManagerError::ViewManagerError(err) => Some(err),
            MetadataManagerError::StatisticsManagerError(err) => Some(err),
        }
    }
}

impl From<TableManagerError> for MetadataManagerError {
    fn from(error: TableManagerError) -> Self {
        MetadataManagerError::TableManagerError(error)
    }
}

impl From<ViewManagerError> for MetadataManagerError {
    fn from(error: ViewManagerError) -> Self {
        MetadataManagerError::ViewManagerError(error)
    }
}

impl From<StatisticsManagerError> for MetadataManagerError {
    fn from(error: StatisticsManagerError) -> Self {
        MetadataManagerError::StatisticsManagerError(error)
    }
}
