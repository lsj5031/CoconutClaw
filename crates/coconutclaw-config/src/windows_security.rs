//! Fail-closed custody checks for native node state, not an ACL repair tool.
use anyhow::{Context, Result, ensure};
use std::{
    fs::{self, File, OpenOptions},
    mem::{size_of, zeroed},
    os::windows::{
        fs::{MetadataExt, OpenOptionsExt},
        io::AsRawHandle,
    },
    path::Path,
    ptr::{addr_of, null_mut},
};
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, LocalFree},
    Security::{
        ACCESS_ALLOWED_ACE, ACE_HEADER, ACL,
        Authorization::{GetSecurityInfo, SE_FILE_OBJECT},
        CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetTokenInformation,
        INHERIT_ONLY_ACE, IsValidAcl, IsValidSid, IsWellKnownSid, OBJECT_INHERIT_ACE,
        OWNER_SECURITY_INFORMATION, TOKEN_QUERY, TOKEN_USER, TokenUser,
        WinBuiltinAdministratorsSid, WinLocalSystemSid,
    },
    Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_ALL_ACCESS, FILE_ATTRIBUTE_DIRECTORY,
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        GetFileInformationByHandle, READ_CONTROL,
    },
    System::{
        SystemServices::ACCESS_ALLOWED_ACE_TYPE,
        Threading::{GetCurrentProcess, OpenProcessToken},
    },
};

pub fn present(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub fn private_directory(path: &Path, allow_missing: bool) -> Result<()> {
    ensure!(path.is_absolute(), "node directory must be absolute");
    let mut nearest = None;
    for ancestor in path.ancestors().collect::<Vec<_>>().into_iter().rev() {
        match fs::symlink_metadata(ancestor) {
            Ok(metadata) => {
                ensure!(
                    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0,
                    "node directory reparse points are not admitted"
                );
                ensure!(metadata.is_dir(), "node path is not a directory");
                nearest = Some(ancestor);
            }
            Err(error) if allow_missing && error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }
    validate_private(nearest.context("node directory has no existing parent")?)
}

struct Token(HANDLE);
impl Drop for Token {
    fn drop(&mut self) {
        unsafe { CloseHandle(self.0) };
    }
}
struct Descriptor(*mut std::ffi::c_void);
impl Drop for Descriptor {
    fn drop(&mut self) {
        unsafe { LocalFree(self.0) };
    }
}

/// Require a real file/directory owned by this account or the privileged OS
/// trustees. Every allow ACE must be this account, SYSTEM or Administrators.
/// Unknown/object/callback/deny ACE layouts fail closed. A current-user full
/// control ACE is required. Inheritance is allowed only when already private.
pub fn validate_private(path: &Path) -> Result<()> {
    let file: File = OpenOptions::new()
        .read(true)
        .access_mode(READ_CONTROL)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
        .context("open node custody metadata")?;
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { zeroed() };
    ensure!(
        unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } != 0,
        "cannot inspect node custody metadata"
    );
    ensure!(
        info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT == 0,
        "node state reparse points are not admitted"
    );
    let mut token = null_mut();
    ensure!(
        unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } != 0,
        "cannot inspect current Windows account"
    );
    let token = Token(token);
    let mut needed = 0;
    unsafe { GetTokenInformation(token.0, TokenUser, null_mut(), 0, &mut needed) };
    ensure!(
        needed > 0 && needed <= 65536,
        "invalid Windows token metadata"
    );
    let mut buffer = vec![0usize; (needed as usize).div_ceil(size_of::<usize>())];
    ensure!(
        unsafe {
            GetTokenInformation(
                token.0,
                TokenUser,
                buffer.as_mut_ptr().cast(),
                needed,
                &mut needed,
            )
        } != 0,
        "cannot read current Windows account"
    );
    let user = unsafe { (*(buffer.as_ptr().cast::<TOKEN_USER>())).User.Sid };
    let trusted = |sid| unsafe {
        EqualSid(sid, user) != 0
            || IsWellKnownSid(sid, WinLocalSystemSid) != 0
            || IsWellKnownSid(sid, WinBuiltinAdministratorsSid) != 0
    };
    let mut owner = null_mut();
    let mut acl: *mut ACL = null_mut();
    let mut descriptor = null_mut();
    let error = unsafe {
        GetSecurityInfo(
            file.as_raw_handle(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            null_mut(),
            &mut acl,
            null_mut(),
            &mut descriptor,
        )
    };
    if error != 0 {
        return Err(std::io::Error::from_raw_os_error(error as i32)).context("inspect node ACL");
    }
    let _descriptor = Descriptor(descriptor);
    ensure!(
        !owner.is_null() && trusted(owner),
        "untrusted node state owner"
    );
    ensure!(
        !acl.is_null() && unsafe { IsValidAcl(acl) } != 0,
        "missing/invalid node DACL"
    );
    let mut own_access = 0;
    let mut inherited_access = 0;
    for index in 0..unsafe { (*acl).AceCount } {
        let mut ace = null_mut();
        ensure!(
            unsafe { GetAce(acl, index as u32, &mut ace) } != 0,
            "invalid node ACE"
        );
        let header = unsafe { &*ace.cast::<ACE_HEADER>() };
        ensure!(
            header.AceType as u32 == ACCESS_ALLOWED_ACE_TYPE
                && header.AceSize as usize >= size_of::<ACCESS_ALLOWED_ACE>(),
            "unsupported node ACE; use the documented private DACL"
        );
        let allowed = unsafe { &*ace.cast::<ACCESS_ALLOWED_ACE>() };
        let sid: windows_sys::Win32::Security::PSID = addr_of!(allowed.SidStart).cast_mut().cast();
        // Bound the variable-length SID inside this ACE before comparison.
        ensure!(header.AceSize >= 16, "short node SID");
        let sid_len = 8 + 4 * unsafe { *sid.cast::<u8>().add(1) as usize };
        ensure!(
            8 + sid_len <= header.AceSize as usize && unsafe { IsValidSid(sid) } != 0,
            "invalid node SID"
        );
        ensure!(
            trusted(sid),
            "unsafe node ACL grants another account access"
        );
        // INHERIT_ONLY_ACE does not grant access on this object.
        if unsafe { EqualSid(sid, user) } != 0
            && allowed.Header.AceFlags as u32 & INHERIT_ONLY_ACE == 0
        {
            own_access |= allowed.Mask;
            let inherit = OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE;
            if allowed.Header.AceFlags as u32 & inherit == inherit {
                inherited_access |= allowed.Mask;
            }
        }
    }
    ensure!(
        own_access & FILE_ALL_ACCESS == FILE_ALL_ACCESS,
        "node ACL requires current-user full control"
    );
    ensure!(
        info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0
            || inherited_access & FILE_ALL_ACCESS == FILE_ALL_ACCESS,
        "node directory ACL requires inheritable current-user full control"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, process::Command};

    #[test]
    fn private_acl_accepts_owner_and_rejects_world_access_and_null_dacl() {
        let root = crate::RuntimeConfig::test_config().data_dir;
        fs::create_dir_all(&root).unwrap();
        let sid = Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "[System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value",
            ])
            .output()
            .unwrap();
        assert!(sid.status.success());
        let sid = String::from_utf8(sid.stdout).unwrap().trim().to_string();
        let output = Command::new("icacls.exe")
            .arg(&root)
            .args([
                "/inheritance:r",
                "/grant:r",
                &format!("*{sid}:(OI)(CI)F"),
                "*S-1-5-18:(OI)(CI)F",
                "*S-1-5-32-544:(OI)(CI)F",
            ])
            .output()
            .unwrap();
        assert!(output.status.success());
        validate_private(&root).unwrap();
        let key = root.join("device-key");
        fs::write(&key, [0u8; 32]).unwrap();
        validate_private(&key).unwrap();
        let output = Command::new("icacls.exe")
            .arg(&root)
            .args(["/grant", "*S-1-1-0:(OI)(CI)R"])
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(validate_private(&root).is_err());
        let inherited = root.join("inherited");
        fs::create_dir(&inherited).unwrap();
        assert!(validate_private(&inherited).is_err());
        assert!(
            Command::new("icacls.exe")
                .arg(&root)
                .args(["/remove:g", "*S-1-1-0"])
                .output()
                .unwrap()
                .status
                .success()
        );
        validate_private(&root).unwrap();
        for file in [&key, &root.join("state.db")] {
            if !file.exists() {
                fs::write(file, []).unwrap();
            }
            let status = Command::new("icacls.exe")
                .arg(file)
                .args(["/grant", "*S-1-1-0:R"])
                .output()
                .unwrap()
                .status;
            assert!(status.success());
            assert!(validate_private(file).is_err());
        }
        use std::os::windows::ffi::OsStrExt;
        let mut name: Vec<u16> = key.as_os_str().encode_wide().chain(Some(0)).collect();
        let error = unsafe {
            windows_sys::Win32::Security::Authorization::SetNamedSecurityInfoW(
                name.as_mut_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                null_mut(),
                null_mut(),
            )
        };
        assert_eq!(error, 0);
        assert!(validate_private(&key).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
