//! Model-controlled attachments are files in the instance's explicit outbox,
//! never arbitrary readable host files. Recheck at send time after recovery.
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use coconutclaw_config::RuntimeConfig;

use crate::markers::{Effect, parse_markers, rejection, render_effects, validate_effects};

pub(crate) const MAX_MEDIA_BYTES: u64 = 20 * 1024 * 1024;

pub(crate) fn outbox(cfg: &RuntimeConfig) -> PathBuf {
    cfg.instance_dir.join("outbox")
}

pub(crate) fn open_attachment(cfg: &RuntimeConfig, path: &Path) -> Result<File> {
    let instance = fs::canonicalize(&cfg.instance_dir)?;
    let root = instance.join("outbox");
    ensure!(!root.is_symlink(), "outbox must not be a symlink");
    let root = fs::canonicalize(root).context("approved outbox is missing")?;
    ensure!(root == instance.join("outbox"), "outbox must not redirect");
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cfg.instance_dir.join(path)
    };
    let canonical = fs::canonicalize(path).context("attachment does not exist")?;
    let relative = canonical
        .strip_prefix(&root)
        .context("attachment is outside approved outbox")?;
    ensure!(!relative.as_os_str().is_empty(), "attachment is not a file");
    let file = open_beneath(&root, relative)?;
    let metadata = file.metadata()?;
    ensure!(metadata.is_file(), "attachment is not a regular file");
    ensure!(
        metadata.len() <= MAX_MEDIA_BYTES,
        "attachment byte limit exceeded"
    );
    Ok(file)
}

#[cfg(unix)]
fn open_beneath(root: &Path, relative: &Path) -> Result<File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::OpenOptionsExt;
    let mut directory = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(root)?;
    let components = relative.components().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        let name = CString::new(component.as_os_str().as_bytes())?;
        let last = index + 1 == components.len();
        let flags = libc::O_RDONLY
            | libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK
            | if last { 0 } else { libc::O_DIRECTORY };
        // Each component is opened relative to an already held directory;
        // no symlink swaps can redirect the later multipart upload.
        let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
        ensure!(fd >= 0, "attachment changed or could not be opened safely");
        let file = unsafe { File::from_raw_fd(fd) };
        if last {
            return Ok(file);
        }
        directory = file;
    }
    anyhow::bail!("attachment is not a file")
}

#[cfg(windows)]
fn open_beneath(root: &Path, relative: &Path) -> Result<File> {
    use std::ffi::OsString;
    use std::os::windows::{ffi::OsStringExt, io::AsRawHandle};
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetFinalPathNameByHandleW(
            file: *mut std::ffi::c_void,
            path: *mut u16,
            length: u32,
            flags: u32,
        ) -> u32;
    }
    let file = File::open(root.join(relative))?;
    let mut buffer = vec![0u16; 32768];
    // Verify the opened handle's target, not another racy path lookup.
    let length = unsafe {
        GetFinalPathNameByHandleW(
            file.as_raw_handle(),
            buffer.as_mut_ptr(),
            buffer.len() as u32,
            0,
        )
    } as usize;
    ensure!(
        length > 0 && length < buffer.len(),
        "cannot verify attachment handle"
    );
    let actual = PathBuf::from(OsString::from_wide(&buffer[..length]));
    ensure!(actual.starts_with(root), "attachment escaped outbox");
    Ok(file)
}

#[cfg(not(any(unix, windows)))]
fn open_beneath(_root: &Path, _relative: &Path) -> Result<File> {
    anyhow::bail!("safe attachment handles are unsupported on this platform")
}

/// Shared by turn persistence, scheduler admission and durable delivery replay.
/// Keep rejections small, durable and visible, with no partial effects.
pub(crate) fn admitted_output(cfg: &RuntimeConfig, output: &str, delegated: bool) -> String {
    let effects = parse_markers(output).to_effects();
    let reason = validate_effects(&effects, delegated).err().or_else(|| {
        effects.iter().find_map(|effect| match effect {
            Effect::SendPhoto(path) | Effect::SendDocument(path) | Effect::SendVideo(path) => {
                open_attachment(cfg, Path::new(path))
                    .err()
                    .map(|_| "media must be a regular file in the approved outbox, at most 20 MiB")
            }
            _ => None,
        })
    });
    if let Some(reason) = reason {
        render_effects(&[Effect::TelegramReply(rejection(reason))])
    } else {
        render_effects(&effects)
    }
}
