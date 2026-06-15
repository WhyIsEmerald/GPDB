use serde::{Deserialize, Serialize};
use xorf::{Filter, Xor8, Xor16};

/// An enum that represents the available filter variants for SSTables.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum FilterVariant {
    /// The XOR8 filter variant.
    Xor8(Xor8),
    /// The XOR16 filter variant.
    Xor16(Xor16),
}

impl FilterVariant {
    /// Checks if the filter contains the given key.
    pub fn contains(&self, key: &u64) -> bool {
        match self {
            Self::Xor8(f) => f.contains(key),
            Self::Xor16(f) => f.contains(key),
        }
    }
}
