//! Per-invocation kernel custody. No PID-tree guesses, breakaway or taskkill.
use anyhow::{Context, Result, ensure};
use std::{
    collections::BTreeMap,
    env,
    ffi::OsStr,
    io::{self, PipeReader, PipeWriter},
    mem::{size_of, size_of_val, zeroed},
    os::windows::{
        ffi::OsStrExt,
        io::{AsRawHandle, OwnedHandle},
        process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
    ptr::{null, null_mut},
    sync::{Arc, Mutex, Weak},
};
use windows_sys::Win32::{
    Foundation::{
        CloseHandle, HANDLE, HANDLE_FLAG_INHERIT, SetHandleInformation, WAIT_OBJECT_0, WAIT_TIMEOUT,
    },
    System::{
        JobObjects::{
            CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
            SetInformationJobObject, TerminateJobObject,
        },
        Threading::{
            CREATE_UNICODE_ENVIRONMENT, CreateProcessW, DeleteProcThreadAttributeList,
            EXTENDED_STARTUPINFO_PRESENT, GetExitCodeProcess, INFINITE,
            InitializeProcThreadAttributeList, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
            PROC_THREAD_ATTRIBUTE_JOB_LIST, PROCESS_INFORMATION, STARTF_USESTDHANDLES,
            STARTUPINFOEXW, UpdateProcThreadAttribute, WaitForSingleObject,
        },
    },
};

struct Handle(HANDLE);
// Kernel handles may be used across threads; ownership closes exactly once.
unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}
impl Drop for Handle {
    fn drop(&mut self) {
        unsafe { CloseHandle(self.0) };
    }
}
static JOBS: Mutex<Vec<Weak<Handle>>> = Mutex::new(Vec::new());

pub(super) struct Job(Arc<Handle>);
impl Job {
    fn new() -> Result<Self> {
        // Null security attributes make the handle noninheritable. The job has
        // no name and no breakaway flags, so only this process owns its lifetime.
        let raw = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if raw.is_null() {
            return Err(std::io::Error::last_os_error()).context("create provider job");
        }
        let handle = Arc::new(Handle(raw));
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if unsafe {
            SetInformationJobObject(
                raw,
                JobObjectExtendedLimitInformation,
                (&info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        } == 0
        {
            return Err(std::io::Error::last_os_error()).context("configure provider job");
        }
        Ok(Self(handle))
    }
    pub(super) fn terminate(&self) -> Result<()> {
        if unsafe { TerminateJobObject(self.0.0, 130) } == 0 {
            return Err(std::io::Error::last_os_error()).context("terminate provider job");
        }
        Ok(())
    }
}
impl Drop for Job {
    fn drop(&mut self) {
        // Also covers setup/wait/I/O errors, before the final handle closes.
        let _ = self.terminate();
    }
}

pub(super) fn kill_active_jobs() {
    for handle in JOBS
        .lock()
        .expect("provider jobs")
        .iter()
        .filter_map(Weak::upgrade)
    {
        unsafe { TerminateJobObject(handle.0, 130) };
    }
}

pub(super) struct Child {
    process: Handle,
    pid: u32,
    pub(super) stdin: Option<PipeWriter>,
    pub(super) stdout: Option<PipeReader>,
    pub(super) stderr: Option<PipeReader>,
}

impl Child {
    pub(super) fn id(&self) -> u32 {
        self.pid
    }

    fn status(&self, timeout: u32) -> io::Result<Option<ExitStatus>> {
        match unsafe { WaitForSingleObject(self.process.0, timeout) } {
            WAIT_TIMEOUT => Ok(None),
            WAIT_OBJECT_0 => {
                let mut code = 0;
                checked(unsafe { GetExitCodeProcess(self.process.0, &mut code) })?;
                Ok(Some(ExitStatus::from_raw(code)))
            }
            _ => Err(io::Error::last_os_error()),
        }
    }
    pub(super) fn try_wait(&self) -> io::Result<Option<ExitStatus>> {
        self.status(0)
    }
    pub(super) fn wait(&self) -> io::Result<ExitStatus> {
        self.status(INFINITE)?
            .ok_or_else(|| io::Error::other("unexpected wait timeout"))
    }
}

fn checked(ok: i32) -> io::Result<()> {
    if ok != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

struct Attributes(Vec<usize>);
impl Drop for Attributes {
    fn drop(&mut self) {
        unsafe { DeleteProcThreadAttributeList(self.0.as_mut_ptr().cast()) };
    }
}

fn wide(value: &OsStr) -> Result<Vec<u16>> {
    let mut value: Vec<_> = value.encode_wide().collect();
    ensure!(!value.contains(&0), "embedded NUL in provider launch");
    value.push(0);
    Ok(value)
}

fn command_line(command: &Command, executable: &Path) -> Result<Vec<u16>> {
    let mut line = vec![34];
    line.extend(executable.as_os_str().encode_wide());
    line.push(34);
    for arg in command.get_args() {
        line.extend([32, 34]);
        let mut slashes = 0;
        for unit in arg.encode_wide() {
            if unit == 92 {
                slashes += 1;
            } else {
                ensure!(unit != 0, "embedded NUL in provider argument");
                let count = if unit == 34 { 2 * slashes + 1 } else { slashes };
                line.extend(std::iter::repeat_n(92, count));
                line.push(unit);
                slashes = 0;
            }
        }
        line.extend(std::iter::repeat_n(92, 2 * slashes));
        line.push(34);
    }
    line.push(0);
    Ok(line)
}

fn launch_inputs(command: &Command, inherit_environment: bool) -> Result<(PathBuf, Vec<u16>)> {
    let mut vars = BTreeMap::new();
    for (name, value) in inherit_environment
        .then(env::vars_os)
        .into_iter()
        .flatten()
        .map(|(name, value)| (name, Some(value)))
        .chain(
            command
                .get_envs()
                .map(|(k, v)| (k.to_owned(), v.map(OsStr::to_owned))),
        )
    {
        let key = name
            .to_str()
            .context("invalid provider environment key")?
            .to_uppercase();
        if let Some(value) = value {
            vars.insert(key, (name, value));
        } else {
            vars.remove(&key);
        }
    }
    let mut program = PathBuf::from(command.get_program());
    if program.extension().is_none() {
        program.set_extension("exe");
    }
    ensure!(
        program
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("exe")),
        "provider requires a native .exe"
    );
    let executable = if program.components().count() == 1 {
        let path = &vars.get("PATH").context("missing provider PATH")?.1;
        env::split_paths(path)
            .filter(|dir| dir.is_absolute())
            .map(|dir| dir.join(&program))
            .find(|path| path.is_file())
            .context("provider executable not found on PATH")?
    } else {
        std::path::absolute(program)?
    };
    let mut block = Vec::new();
    for (mut name, value) in vars.into_values() {
        name.push("=");
        name.push(value);
        block.extend(wide(&name)?);
    }
    if block.is_empty() {
        block.push(0);
    }
    block.push(0);
    Ok((executable, block))
}

pub(super) fn spawn(
    command: &Command,
    input: bool,
    inherit_environment: bool,
) -> Result<(Child, Job)> {
    let (executable, environment) = launch_inputs(command, inherit_environment)?;
    let mut line = command_line(command, &executable)?;
    let app = wide(executable.as_os_str())?;
    let cwd = command
        .get_current_dir()
        .map(|p| wide(p.as_os_str()))
        .transpose()?;
    let job = Job::new()?;
    let (read_in, write_in) = io::pipe()?;
    let (stdout, write_out) = io::pipe()?;
    let (stderr, write_err) = io::pipe()?;
    let child_ends: [OwnedHandle; 3] = [read_in.into(), write_out.into(), write_err.into()];
    let handles = child_ends.each_ref().map(|handle| handle.as_raw_handle());
    for handle in handles {
        checked(unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT) })?;
    }
    let mut bytes = 0;
    unsafe { InitializeProcThreadAttributeList(null_mut(), 2, 0, &mut bytes) };
    ensure!(bytes > 0, "cannot size process attributes");
    let mut storage = vec![0usize; bytes.div_ceil(size_of::<usize>())];
    let list = storage.as_mut_ptr().cast();
    checked(unsafe { InitializeProcThreadAttributeList(list, 2, 0, &mut bytes) })?;
    let _attributes = Attributes(storage);
    for (attribute, values) in [
        (
            PROC_THREAD_ATTRIBUTE_JOB_LIST,
            std::slice::from_ref(&job.0.0),
        ),
        (PROC_THREAD_ATTRIBUTE_HANDLE_LIST, handles.as_slice()),
    ] {
        checked(unsafe {
            UpdateProcThreadAttribute(
                list,
                0,
                attribute as usize,
                values.as_ptr().cast(),
                size_of_val(values),
                null_mut(),
                null(),
            )
        })
        .context("configure contained provider creation")?;
    }
    let mut start: STARTUPINFOEXW = unsafe { zeroed() };
    start.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
    start.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    [
        start.StartupInfo.hStdInput,
        start.StartupInfo.hStdOutput,
        start.StartupInfo.hStdError,
    ] = handles;
    start.lpAttributeList = list;
    let mut process: PROCESS_INFORMATION = unsafe { zeroed() };
    let mut jobs = JOBS.lock().expect("provider jobs");
    jobs.retain(|job| job.strong_count() != 0);
    checked(unsafe {
        CreateProcessW(
            app.as_ptr(),
            line.as_mut_ptr(),
            null(),
            null(),
            1,
            EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT,
            environment.as_ptr().cast(),
            cwd.as_ref().map_or(null(), |cwd| cwd.as_ptr()),
            &start.StartupInfo,
            &mut process,
        )
    })
    .context("create provider inside job")?;
    drop(Handle(process.hThread));
    jobs.push(Arc::downgrade(&job.0));
    Ok((
        Child {
            process: Handle(process.hProcess),
            pid: process.dwProcessId,
            stdin: input.then_some(write_in),
            stdout: Some(stdout),
            stderr: Some(stderr),
        },
        job,
    ))
}

#[cfg(test)]
mod tests;
