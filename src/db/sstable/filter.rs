use serde::{Deserialize, Serialize};
use xorf::{Filter, Xor8, Xor16};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum FilterVariant {
    Xor8(Xor8),
    Xor16(Xor16),
}

impl FilterVariant {
    pub fn contains(&self, key: &u64) -> bool {
        match self {
            Self::Xor8(f) => f.contains(key),
            Self::Xor16(f) => f.contains(key),
        }
    }
}
