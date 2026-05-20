use anyhow::{Context, Result};
use coconutclaw_config::RuntimeConfig;
use notify::event::{CreateKind, ModifyKind};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::fs;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};

use crate::types::CancelSignal;

pub(crate) fn cancel_marker_path(cfg: &RuntimeConfig) -> PathBuf {
    cfg.runtime_dir.join("cancel")
}

pub(crate) fn signal_cancel_marker(cfg: &RuntimeConfig) -> Result<()> {
    let path = cancel_marker_path(cfg);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(&path, "").with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

pub(crate) fn clear_cancel_marker(cfg: &RuntimeConfig) {
    let path = cancel_marker_path(cfg);
    let _ = fs::remove_file(path);
}

/// A long-lived cancel watcher that uses `notify` to watch the cancel marker file.
///
/// When the marker file is created or modified, all registered cancel flags are signalled.
/// This replaces the previous per-turn 150ms polling thread with an event-driven approach.
pub(crate) struct CancelRouter {
    registry: Arc<Mutex<Vec<Weak<AtomicBool>>>>,
    _watcher: RecommendedWatcher,
    marker_path: PathBuf,
}

impl std::fmt::Debug for CancelRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancelRouter")
            .field("marker_path", &self.marker_path)
            .finish()
    }
}

impl CancelRouter {
    /// Start watching the cancel marker file.
    ///
    /// Spawns a notify watcher that listens for file creation/modification events
    /// on the cancel marker path. The watcher thread lives as long as the returned
    /// `Arc<CancelRouter>`.
    pub(crate) fn start(cfg: &RuntimeConfig) -> Result<Arc<Self>> {
        let marker_path = cancel_marker_path(cfg);
        if let Some(parent) = marker_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        // Watch the parent directory and filter for the marker file name.
        let watch_dir = marker_path
            .parent()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        let marker_name = marker_path
            .file_name()
            .and_then(|n| n.to_str())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| "cancel".to_string());

        // Shared registry: the watcher callback and CancelRouter both hold a clone.
        let registry = Arc::new(Mutex::new(Vec::<Weak<AtomicBool>>::new()));

        let registry_for_watcher = Arc::clone(&registry);
        let marker_name_for_watcher = marker_name.clone();

        let mut watcher = RecommendedWatcher::new(
            move |res: notify::Result<Event>| {
                if let Ok(event) = res {
                    let is_create_or_modify = matches!(
                        event.kind,
                        EventKind::Create(CreateKind::File)
                            | EventKind::Modify(ModifyKind::Data(_))
                            | EventKind::Modify(ModifyKind::Any)
                    );
                    if !is_create_or_modify {
                        return;
                    }
                    let matches_marker = event.paths.iter().any(|p| {
                        p.file_name()
                            .map(|n| {
                                n.to_string_lossy().as_ref() == marker_name_for_watcher.as_str()
                            })
                            .unwrap_or(false)
                    });
                    if !matches_marker {
                        return;
                    }
                    signal_registry(&registry_for_watcher);
                }
            },
            Config::default(),
        )?;

        watcher.watch(&watch_dir, RecursiveMode::NonRecursive)?;

        Ok(Arc::new(Self {
            registry,
            _watcher: watcher,
            marker_path,
        }))
    }

    /// Register a cancel flag to be signalled when the marker file is touched.
    ///
    /// The flag is stored as a `Weak` reference; when the owner drops its `Arc`,
    /// the entry is automatically skipped on the next signal pass.
    pub(crate) fn register(self: &Arc<Self>, flag: Arc<AtomicBool>) {
        let mut guard = self.registry.lock().expect("cancel registry");
        guard.retain(|w| w.strong_count() > 0);
        guard.push(Arc::downgrade(&flag));
    }

    /// The path being watched (for debugging / logging).
    #[allow(dead_code)]
    pub(crate) fn marker_path(&self) -> &PathBuf {
        &self.marker_path
    }
}

/// Signal all live cancel flags in the registry.
fn signal_registry(registry: &Mutex<Vec<Weak<AtomicBool>>>) {
    let mut guard = registry.lock().expect("cancel registry");
    // Clean up dead entries
    guard.retain(|w| w.strong_count() > 0);
    for weak in guard.iter() {
        if let Some(flag) = weak.upgrade() {
            flag.store(true, Ordering::SeqCst);
        }
    }
}

// ── Legacy helpers kept for tests ───────────────────────────────────────

#[allow(dead_code)]
pub(crate) fn cancel_signal_from_update(
    value: &serde_json::Value,
    expected_chat_id: &str,
) -> Option<CancelSignal> {
    use crate::webhook::value_to_string;

    if let Some(callback_query) = value.get("callback_query") {
        let data = callback_query
            .get("data")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let chat_id = callback_query
            .get("message")
            .and_then(|node| node.get("chat"))
            .and_then(|node| node.get("id"))
            .map(value_to_string)
            .unwrap_or_default();
        if data.eq_ignore_ascii_case("cancel") && chat_id == expected_chat_id {
            let callback_query_id = callback_query
                .get("id")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned);
            return Some(CancelSignal { callback_query_id });
        }
    }

    if let Some(message) = value.get("message") {
        let chat_id = message
            .get("chat")
            .and_then(|node| node.get("id"))
            .map(value_to_string)
            .unwrap_or_default();
        let text = message
            .get("text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if chat_id == expected_chat_id && text.trim().eq_ignore_ascii_case("/cancel") {
            return Some(CancelSignal {
                callback_query_id: None,
            });
        }
    }

    None
}
