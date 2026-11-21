use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DeviceIdentity {
    pub desktop_id: String,
    pub desktop_name: String,
    pub device_key: String,
}

impl DeviceIdentity {
    pub(crate) fn new() -> Self {
        Self {
            desktop_id: uuid::Uuid::new_v4().simple().to_string(),
            desktop_name: std::env::var("COMPUTERNAME")
                .or_else(|_| std::env::var("HOSTNAME"))
                .unwrap_or_else(|_| "Desktop".into()),
            device_key: uuid::Uuid::new_v4().simple().to_string(),
        }
    }

    pub(crate) fn load(path: &Path) -> io::Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => {
                let identity: Self = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                if identity.desktop_id.is_empty() || identity.device_key.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "empty relay device identity",
                    ));
                }
                Ok(identity)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let identity = Self::new();
                let parent = path.parent().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "device identity path has no parent",
                    )
                })?;
                std::fs::create_dir_all(parent)?;
                let temporary = parent.join(format!(".relay-device-{}.tmp", uuid::Uuid::new_v4()));
                let mut options = std::fs::OpenOptions::new();
                options.write(true).create_new(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                use std::io::Write;
                let result = (|| {
                    let mut file = options.open(&temporary)?;
                    file.write_all(&serde_json::to_vec(&identity).map_err(io::Error::other)?)?;
                    file.sync_all()?;
                    std::fs::hard_link(&temporary, path)
                })();
                let _ = std::fs::remove_file(&temporary);
                match result {
                    Ok(()) => Ok(identity),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Self::load(path),
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_survives_reload_and_corruption_is_not_silently_replaced() {
        let root = std::env::temp_dir().join(format!("mt-device-{}", uuid::Uuid::new_v4()));
        let path = root.join("relay-device.json");
        let first = DeviceIdentity::load(&path).unwrap();
        let second = DeviceIdentity::load(&path).unwrap();
        assert_eq!(first.desktop_id, second.desktop_id);
        assert_eq!(first.device_key, second.device_key);
        let corrupt = b"not a device identity";
        std::fs::write(&path, corrupt).unwrap();
        assert!(DeviceIdentity::load(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), corrupt);
        assert!(
            root.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("mt-device-")
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
