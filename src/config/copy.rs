use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct CopyConfig {
    pub preserve_mode: bool,
}

impl Default for CopyConfig {
    fn default() -> Self {
        Self {
            preserve_mode: true,
        }
    }
}
