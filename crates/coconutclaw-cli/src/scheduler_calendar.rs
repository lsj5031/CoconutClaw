//! Deadline/event intake owned by SessionScheduler, never a second executor.
use super::*;
#[cfg(target_os = "macos")]
use notify::KqueueWatcher as CalendarWatcher;
#[cfg(not(target_os = "macos"))]
use notify::RecommendedWatcher as CalendarWatcher;
use notify::{Event, EventKind, RecursiveMode, Watcher};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

impl SessionScheduler {
    pub(crate) fn start_calendar(&self) -> Result<()> {
        let _lifecycle = self.inner.lifecycle.lock().expect("scheduler lifecycle");
        anyhow::ensure!(
            !self.inner.stopping.load(Ordering::SeqCst),
            "runtime is stopping"
        );
        let mut wake = self.inner.calendar_wake.lock().expect("calendar wake");
        if wake.is_some() {
            return Ok(());
        }
        // One connection, moved to one owner. In particular do not reinitialize
        // Store on every watcher event (schema/backfill writes would self-wake).
        let store = Store::connect(&self.inner.cfg)?;
        let path = self.inner.cfg.sqlite_db_path.clone();
        let parent = path.parent().context("database parent missing")?;
        let name = path
            .file_name()
            .context("database name missing")?
            .to_os_string();
        let wal = format!("{}-wal", name.to_string_lossy());
        let journal = format!("{}-journal", name.to_string_lossy());
        #[cfg(target_os = "macos")]
        let watched_files = [name.clone(), wal.clone().into(), journal.clone().into()];
        let (send, receive) = mpsc::sync_channel(1);
        let signal = send.clone();
        let mut watcher = CalendarWatcher::new(
            move |event: notify::Result<Event>| {
                let changed = match event {
                    Ok(event) => {
                        !matches!(event.kind, EventKind::Access(_))
                            && event.paths.iter().any(|p| {
                                p.file_name().is_some_and(|n| {
                                    n == name
                                        || n == std::ffi::OsStr::new(&wal)
                                        || n == std::ffi::OsStr::new(&journal)
                                })
                            })
                    }
                    Err(error) => {
                        tracing::warn!(
                            "calendar watcher error, deadline rebase remains active: {error}"
                        );
                        true
                    }
                };
                if changed {
                    let _ = signal.try_send(());
                }
            },
            notify::Config::default(),
        )?;
        // FSEvents requires the physical directory, not macOS's /var or /tmp
        // symlink spelling. Keep the database-name event filter unchanged.
        let physical_parent = parent.canonicalize()?;
        watcher.watch(&physical_parent, RecursiveMode::NonRecursive)?;
        // FSEvents can defer writes until an open SQLite WAL is closed. Vnode
        // notifications must watch the existing files, not only directory entries.
        #[cfg(target_os = "macos")]
        for file in watched_files {
            let file = physical_parent.join(file);
            if file.exists() {
                watcher.watch(&file, RecursiveMode::NonRecursive)?;
            }
        }
        let weak = Arc::downgrade(&self.inner);
        let worker = thread::spawn(move || {
            let _watcher = watcher;
            let mut last = Instant::now() - Duration::from_millis(100);
            loop {
                // Coalesce write bursts and bound work even under continuous
                // external writes. The signal remains queued during this gap.
                thread::sleep(Duration::from_millis(100).saturating_sub(last.elapsed()));
                last = Instant::now();
                let wait = {
                    let Some(inner) = weak.upgrade() else { break };
                    if inner.stopping.load(Ordering::SeqCst) {
                        break;
                    }
                    let scheduler = Self { inner };
                    let step = scheduler
                        .wake_due_with_store(&store, chrono::Utc::now())
                        .and_then(|_| store.next_schedule_deadline());
                    match step {
                        Ok(next) => next
                            .and_then(chrono::DateTime::from_timestamp_secs)
                            .map(|at| (at - chrono::Utc::now()).to_std().unwrap_or_default())
                            // Rebase after clock jumps/lost filesystem events;
                            // not tied to Telegram or its error/retry sleeps.
                            .unwrap_or(Duration::from_secs(30))
                            .min(Duration::from_secs(30)),
                        Err(error) => {
                            tracing::warn!(
                                "calendar admission pending, no effect retried: {error:#}"
                            );
                            // Signals cannot turn a failing transaction into a
                            // hot loop; shutdown still joins within the bound.
                            thread::sleep(Duration::from_secs(1));
                            Duration::ZERO
                        }
                    }
                };
                match receive.recv_timeout(wait) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        });
        *wake = Some(send);
        self.inner.workers.lock().expect("workers").push(worker);
        Ok(())
    }
}
