use std::fs;
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

pub fn write_fake_provider_script(
    dir: &Path,
    stem: &str,
    unix_body: String,
    windows_body: String,
) -> String {
    let (path, body, command) = if cfg!(windows) {
        let path = dir.join(format!("{stem}.ps1"));
        let command = format!(
            "powershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"",
            path.display()
        );
        (path, windows_body, command)
    } else {
        let path = dir.join(format!("{stem}.sh"));
        let command = path.display().to_string();
        (path, unix_body, command)
    };

    fs::write(&path, &body).unwrap();

    #[cfg(unix)]
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();

    command
}
