use std::fs;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

pub fn write_fake_provider_script(
    dir: &Path,
    stem: &str,
    unix_body: String,
    windows_body: String,
) -> PathBuf {
    let (path, body) = if cfg!(windows) {
        (dir.join(format!("{stem}.cmd")), windows_body)
    } else {
        (dir.join(format!("{stem}.sh")), unix_body)
    };

    fs::write(&path, &body).unwrap();

    #[cfg(unix)]
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();

    path
}
