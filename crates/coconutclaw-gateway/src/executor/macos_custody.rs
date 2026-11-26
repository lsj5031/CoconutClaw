use std::ffi::OsString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};

use super::{Deadline, fixture_command, now_ms};

pub(super) const MODE: &str = "--fixture-macos-child";

pub(super) fn command(attempt: i64, hold: bool, deadline: &Deadline) -> Result<Command> {
    let clock = uptime()?;
    let expires = clock + deadline.monotonic.saturating_duration_since(Instant::now());
    let mut command = Command::new(std::env::current_exe()?);
    command.args([
        MODE,
        &std::process::id().to_string(),
        &attempt.to_string(),
        if hold { "hold" } else { "reply" },
        &deadline.wall.to_string(),
        &expires.as_millis().to_string(),
    ]);
    Ok(command)
}

pub(super) fn run(args: impl Iterator<Item = OsString>) -> Result<()> {
    let args: Vec<_> = args.collect();
    let [owner, attempt, behavior, wall, expires] = args.as_slice() else {
        anyhow::bail!("invalid fixture child arguments");
    };
    let owner: libc::pid_t = owner.to_str().context("invalid owner")?.parse()?;
    let attempt = attempt.to_str().context("invalid attempt")?.parse()?;
    let wall = wall.to_str().context("invalid wall deadline")?.parse()?;
    let expires = expires
        .to_str()
        .context("invalid monotonic deadline")?
        .parse()?;
    ensure!(
        behavior == "hold" || behavior == "reply",
        "invalid fixture child behavior"
    );
    ensure!(
        owner > 1 && unsafe { libc::getpgrp() == libc::getpid() && libc::getppid() == owner },
        "fixture child must lead its runner-owned process group"
    );
    let queue = unsafe { libc::kqueue() };
    if queue < 0 {
        return Err(io::Error::last_os_error()).context("create fixture custody queue");
    }
    let queue = unsafe { OwnedFd::from_raw_fd(queue) };
    let events = [
        event(owner as libc::uintptr_t, libc::EVFILT_PROC, libc::NOTE_EXIT),
        event(libc::SIGTERM as libc::uintptr_t, libc::EVFILT_SIGNAL, 0),
    ];
    let registered = unsafe {
        libc::kevent(
            queue.as_raw_fd(),
            events.as_ptr(),
            2,
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
        )
    };
    if registered < 0 {
        return Err(io::Error::last_os_error()).context("register fixture custody events");
    }
    ensure!(
        unsafe { libc::getppid() } == owner,
        "fixture owner exited before launch"
    );
    ensure!(
        unsafe { libc::signal(libc::SIGTERM, libc::SIG_IGN) } != libc::SIG_ERR,
        "configure fixture cancellation"
    );
    ensure!(
        now_ms()? < wall && uptime()?.as_millis() < expires,
        "start grant expired"
    );

    let mut child = fixture_command(attempt, behavior == "hold")
        .stdin(Stdio::inherit())
        .spawn()?;
    let timeout = libc::timespec {
        tv_sec: 0,
        tv_nsec: 50_000_000,
    };
    let mut ready = std::mem::MaybeUninit::uninit();
    loop {
        let received = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                std::ptr::null(),
                0,
                ready.as_mut_ptr(),
                1,
                &timeout,
            )
        };
        if received > 0
            || received < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted
        {
            kill_group();
        }
        match child.try_wait() {
            Ok(Some(status)) => std::process::exit(status.code().unwrap_or(1)),
            Ok(None) => {}
            Err(_) => kill_group(),
        }
    }
}

fn event(ident: libc::uintptr_t, filter: i16, fflags: u32) -> libc::kevent {
    libc::kevent {
        ident,
        filter,
        flags: libc::EV_ADD,
        fflags,
        data: 0,
        udata: std::ptr::null_mut(),
    }
}

fn uptime() -> Result<Duration> {
    let mut clock = std::mem::MaybeUninit::uninit();
    if unsafe { libc::clock_gettime(libc::CLOCK_UPTIME_RAW, clock.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error()).context("read fixture monotonic clock");
    }
    let clock = unsafe { clock.assume_init() };
    Ok(Duration::new(
        clock.tv_sec.try_into()?,
        clock.tv_nsec.try_into()?,
    ))
}

fn kill_group() -> ! {
    unsafe { libc::kill(0, libc::SIGKILL) };
    std::process::abort()
}
