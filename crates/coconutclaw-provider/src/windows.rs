//! Per-invocation kernel custody. No PID-tree guesses, breakaway or taskkill.
use anyhow::{Context, Result, bail};
use std::{
    mem::{size_of, zeroed},
    os::windows::{io::AsRawHandle, process::CommandExt},
    process::{Child, Command},
    sync::{Arc, Mutex, Weak},
};
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE},
    System::{
        Diagnostics::ToolHelp::{
            CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
        },
        JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
            SetInformationJobObject, TerminateJobObject,
        },
        Threading::{CREATE_SUSPENDED, OpenThread, ResumeThread, THREAD_SUSPEND_RESUME},
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
        let mut jobs = JOBS.lock().expect("provider jobs");
        jobs.retain(|job| job.strong_count() != 0);
        jobs.push(Arc::downgrade(&handle));
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

pub(super) fn spawn(command: &mut Command) -> Result<(Child, Job)> {
    let job = Job::new()?;
    command.creation_flags(CREATE_SUSPENDED);
    let mut child = command.spawn().context("spawn suspended provider")?;
    let admit = || -> Result<()> {
        if unsafe { AssignProcessToJobObject(job.0.0, child.as_raw_handle()) } == 0 {
            return Err(std::io::Error::last_os_error()).context("assign provider job");
        }
        // std closes its primary-thread handle. The suspended process has
        // executed no code and cannot spawn another thread; find that thread
        // using documented APIs, with the process handle still held.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            return Err(std::io::Error::last_os_error()).context("snapshot suspended thread");
        }
        let snapshot = Handle(snapshot);
        let mut entry: THREADENTRY32 = unsafe { zeroed() };
        entry.dwSize = size_of::<THREADENTRY32>() as u32;
        let mut more = unsafe { Thread32First(snapshot.0, &mut entry) };
        while more != 0 {
            if entry.th32OwnerProcessID == child.id() {
                let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
                if thread.is_null() {
                    return Err(std::io::Error::last_os_error()).context("open suspended thread");
                }
                let thread = Handle(thread);
                let previous = unsafe { ResumeThread(thread.0) };
                if previous == u32::MAX {
                    return Err(std::io::Error::last_os_error())
                        .context("resume contained provider");
                }
                if previous != 1 {
                    bail!("unexpected suspended provider thread state");
                }
                return Ok(());
            }
            more = unsafe { Thread32Next(snapshot.0, &mut entry) };
        }
        bail!("suspended provider thread not found")
    };
    if let Err(error) = admit() {
        // No provider instruction ran if admission failed. Terminate even when
        // job assignment failed, then reap the direct suspended process.
        let _ = job.terminate();
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }
    Ok((child, job))
}

#[cfg(test)]
mod tests;
