use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

// no docs
// no comments
// no error handlings
// no variable name edit
/// Represents a constant value stored in the database.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Constant {
    Int(i32),
    Str(String),
}
impl Constant {
    /// Convert the Constant to an integer.
    /// Panics if the Constant is not an integer.
    pub fn as_int(&self) -> i32 {
        if let Constant::Int(val) = self {
            *val
        } else {
            panic!("Called as_int on a non-Int Constant");
        }
    }

    /// Convert the Constant to a string.
    /// Panics if the Constant is not a string.
    pub fn as_str(&self) -> &str {
        if let Constant::Str(val) = self {
            val
        } else {
            panic!("Called as_str on a non-Str Constant");
        }
    }

    /// Compares two Constant values.
    /// Returns an `Option<Ordering>` that depends on the values being compared.
    pub fn compare(&self, other: &Constant) -> Option<Ordering> {
        match (self, other) {
            (Constant::Int(a), Constant::Int(b)) => Some(a.cmp(b)),
            (Constant::Str(a), Constant::Str(b)) => Some(a.cmp(b)),
            _ => None, // Incomparable types
        }
    }

    pub fn hash_code(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl std::fmt::Display for Constant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Constant::Int(val) => write!(f, "{}", val),
            Constant::Str(ref val) => write!(f, "{}", val),
        }
    }
}
