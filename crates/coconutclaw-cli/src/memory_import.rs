//! Explicit operator-only import. Source text never selects authority or runs tools.
use anyhow::{Context, Result, bail, ensure};
use clap::Args;
use coconutclaw_config::{RuntimeConfig, ensure_instance_layout};
use rusqlite::params;
use serde_json::json;
use std::{
    collections::BTreeSet,
    fs::File,
    io::Read,
    path::{Component, Path, PathBuf},
};

use crate::store::Store;

#[derive(Args, Debug)]
pub(crate) struct ImportArgs {
    #[arg(long)]
    source_root: PathBuf,
    #[arg(long)]
    chat_id: String,
    #[arg(long)]
    topic_id: Option<i32>,
    #[arg(long = "file", required = true)]
    files: Vec<PathBuf>,
    #[arg(long)]
    recursive: bool,
    #[arg(long, conflicts_with = "dry_run")]
    apply: bool,
    #[arg(long)]
    dry_run: bool,
    #[arg(long, requires = "apply")]
    promote_curated: bool,
}

#[derive(Debug)]
struct Chunk {
    path: String,
    ordinal: usize,
    first: usize,
    last: usize,
    content: String,
}

struct Preview {
    root: String,
    scope: String,
    chunks: Vec<Chunk>,
    files: usize,
    bytes: usize,
}

fn scope(cfg: &RuntimeConfig, args: &ImportArgs) -> Result<String> {
    let id = args
        .chat_id
        .parse::<i64>()
        .context("invalid private chat ID")?;
    ensure!(
        id > 0 && id < (1i64 << 52) && id.to_string() == args.chat_id,
        "expected canonical positive private chat ID"
    );
    ensure!(
        crate::util::is_allowed_chat(cfg, Some(&args.chat_id)),
        "destination chat is not authorized"
    );
    ensure!(args.topic_id.is_none_or(|id| id > 0), "invalid topic ID");
    Ok(crate::session::SessionKey {
        platform: crate::session::SessionPlatform::Telegram,
        root_id: args.chat_id.clone(),
        thread_id: args.topic_id.map(|id| id.to_string()),
    }
    .id())
}

fn selected_path(path: &Path, recursive: bool) -> Result<String> {
    ensure!(
        path.as_os_str().len() <= 240,
        "source path exceeds 240 bytes"
    );
    let parts = path
        .components()
        .map(|c| match c {
            Component::Normal(s) => s.to_str().context("paths must be UTF-8"),
            _ => bail!("source traversal/absolute paths are forbidden"),
        })
        .collect::<Result<Vec<_>>>()?;
    let text = parts.join("/");
    ensure!(text.len() <= 240, "source path exceeds 240 bytes");
    ensure!(
        text == "USER.md"
            || text == "MEMORY.md"
            || (parts.first() == Some(&"memory")
                && parts.len() >= 2
                && (recursive || parts.len() == 2)
                && text.ends_with(".md")),
        "select USER.md, MEMORY.md or memory/*.md; nested files require --recursive"
    );
    for part in parts {
        let lower = part.to_lowercase();
        ensure!(
            !part.starts_with('.')
                && !part.chars().any(char::is_control)
                && ![
                    "secret",
                    "credential",
                    "token",
                    "auth",
                    "password",
                    "dream",
                    "archive"
                ]
                .iter()
                .any(|s| lower.contains(s)),
            "hidden, generated/archive or credential-like paths are forbidden"
        );
    }
    Ok(text)
}

/// Directory handles and openat(O_NOFOLLOW) keep every component pinned, even
/// if an untrusted process swaps a parent directory during traversal.
#[cfg(unix)]
fn open_child(parent: &File, name: &std::ffi::OsStr, directory: bool) -> Result<File> {
    use std::os::{
        fd::{AsRawFd, FromRawFd},
        unix::ffi::OsStrExt,
    };
    let name = std::ffi::CString::new(name.as_bytes())?;
    let flags = libc::O_RDONLY
        | libc::O_NOFOLLOW
        | libc::O_CLOEXEC
        | libc::O_NONBLOCK
        | if directory { libc::O_DIRECTORY } else { 0 };
    // SAFETY: valid C string and live borrowed directory fd; ownership of the
    // returned descriptor transfers exactly once to File.
    let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("cannot open selected source safely");
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(not(unix))]
fn open_child(_: &File, _: &std::ffi::OsStr, _: bool) -> Result<File> {
    bail!("safe explicit import currently requires Unix directory handles")
}

fn open_root(path: &Path) -> Result<(File, String)> {
    ensure!(path.is_absolute(), "--source-root must be absolute");
    let root = path.to_str().context("source root must be UTF-8")?;
    ensure!(root.len() <= 1024, "source root exceeds 1024 bytes");
    let mut dir = File::open("/")?;
    for part in path.components() {
        match part {
            Component::RootDir => {}
            Component::Normal(name) => dir = open_child(&dir, name, true)?,
            _ => bail!("source root must not contain traversal"),
        }
    }
    Ok((
        dir,
        path.components()
            .collect::<PathBuf>()
            .to_string_lossy()
            .into_owned(),
    ))
}

fn chunks(path: &str, text: &str) -> Result<Vec<Chunk>> {
    ensure!(
        !text
            .chars()
            .any(|c| c.is_control() && c != '\n' && c != '\t' && c != '\r'),
        "source has unsupported control characters"
    );
    let mut output = Vec::new();
    let mut content = String::new();
    let mut first = 1;
    let mut last = 1;
    let flush = |content: &mut String, first, last, output: &mut Vec<Chunk>| {
        let trimmed = content.trim().to_string();
        content.clear();
        if !trimmed.is_empty() {
            output.push(Chunk {
                path: path.into(),
                ordinal: output.len(),
                first,
                last,
                content: trimmed,
            });
        }
    };
    for (index, line) in text.lines().enumerate() {
        let number = index + 1;
        if line.trim().is_empty() || line.starts_with('#') {
            flush(&mut content, first, last, &mut output);
        }
        if line.trim().is_empty() {
            continue;
        }
        if !content.is_empty() {
            if content.len() + 1 > 2048 {
                flush(&mut content, first, last, &mut output);
            } else {
                content.push('\n');
            }
        }
        for ch in line.chars() {
            if content.len() + ch.len_utf8() > 2048 {
                flush(&mut content, first, last, &mut output);
            }
            if content.is_empty() {
                first = number;
            }
            content.push(ch);
            last = number;
        }
    }
    flush(&mut content, first, last, &mut output);
    Ok(output)
}

fn preview(cfg: &RuntimeConfig, args: &ImportArgs) -> Result<Preview> {
    let scope = scope(cfg, args)?;
    ensure!(args.files.len() <= 32, "at most 32 selected files");
    let (root, root_name) = open_root(&args.source_root)?;
    let mut paths = BTreeSet::new();
    for path in &args.files {
        ensure!(
            paths.insert(selected_path(path, args.recursive)?),
            "duplicate selected path"
        );
    }
    let mut output = Preview {
        root: root_name,
        scope,
        chunks: Vec::new(),
        files: paths.len(),
        bytes: 0,
    };
    for path in paths {
        let parts: Vec<_> = Path::new(&path).components().collect();
        let mut parent = root.try_clone()?;
        for part in &parts[..parts.len() - 1] {
            parent = open_child(&parent, part.as_os_str(), true)?;
        }
        let file = open_child(
            &parent,
            parts.last().expect("validated path").as_os_str(),
            false,
        )?;
        ensure!(
            file.metadata()?.is_file(),
            "selected source is not a regular file"
        );
        ensure!(file.metadata()?.len() <= 65536, "source exceeds 64 KiB");
        let mut bytes = Vec::new();
        file.take(65537).read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= 65536, "source exceeds 64 KiB");
        output.bytes += bytes.len();
        ensure!(output.bytes <= 1048576, "selected corpus exceeds 1 MiB");
        let text = std::str::from_utf8(&bytes).context("source must be UTF-8 Markdown")?;
        output.chunks.extend(chunks(&path, text)?);
        ensure!(
            output.chunks.len() <= 1024,
            "at most 1024 chunks per import"
        );
    }
    if args.promote_curated {
        ensure!(
            output.chunks.len() <= 4 && output.chunks.iter().all(|c| c.content.len() <= 512),
            "curated import permits at most four chunks of at most 512 bytes"
        );
    }
    Ok(output)
}

fn fingerprint(text: &str) -> String {
    let hash = text.bytes().fold(0xcbf29ce484222325u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    format!("fnv1a64:{hash:016x}")
}

fn apply(store: &Store, preview: &Preview, curated: bool) -> Result<(usize, usize)> {
    let tx = rusqlite::Transaction::new_unchecked(
        &store.conn,
        rusqlite::TransactionBehavior::Immediate,
    )?;
    let mut inserted = 0;
    let mut skipped = 0;
    for chunk in &preview.chunks {
        // Compare only the latest source receipt, independently of effective
        // state. Undo survives replay; a later deliberate source reversion is new.
        let seen: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM memory_import_origins o JOIN memory_records r ON r.id=o.record_id
             WHERE o.scope=?1 AND o.root=?2 AND o.path=?3 AND o.chunk=?4 AND r.content=?5
               AND o.id=(SELECT MAX(n.id) FROM memory_import_origins n
                   WHERE n.scope=o.scope AND n.root=o.root AND n.path=o.path AND n.chunk=o.chunk))",
            params![preview.scope, preview.root, chunk.path, chunk.ordinal, chunk.content],
            |r| r.get(0),
        )?;
        if seen {
            skipped += 1;
            continue;
        }
        let active = {
            let mut statement = tx.prepare(
                "SELECT r.id,r.curated FROM memory_import_origins o JOIN memory_records r ON r.import_origin=o.id
                 JOIN memory_active a ON a.record_id=r.id
                 WHERE o.scope=?1 AND o.root=?2 AND o.path=?3 AND o.chunk=?4 ORDER BY o.id DESC LIMIT 2",
            )?;
            statement
                .query_map(
                    params![preview.scope, preview.root, chunk.path, chunk.ordinal],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, bool>(1)?)),
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        ensure!(
            active.len() <= 1,
            "multiple restored versions active for source slot; resolve before import"
        );
        let old = active.first().copied();
        let curated = old.map_or(curated, |(_, curated)| curated);
        ensure!(
            !curated || chunk.content.len() <= 512,
            "curated source correction exceeds 512 bytes"
        );
        // Origin and record reference each other, both are committed atomically.
        let record_id: i64 = tx.query_row(
            "SELECT COALESCE(MAX(id),0)+1 FROM memory_records",
            [],
            |r| r.get(0),
        )?;
        tx.execute(
            "INSERT INTO memory_import_origins(scope,root,path,chunk,first_line,last_line,fingerprint,record_id)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![preview.scope,preview.root,chunk.path,chunk.ordinal,chunk.first,chunk.last,fingerprint(&chunk.content),record_id],
        )?;
        let origin_id = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO memory_records(id,scope,import_origin,content,curated) VALUES(?1,?2,?3,?4,?5)",
            params![record_id,preview.scope,origin_id,chunk.content,curated],
        )?;
        if let Some((old, _)) = old {
            tx.execute("DELETE FROM memory_active WHERE record_id=?1", [old])?;
        }
        if old.is_some() || curated {
            tx.execute(
                "INSERT INTO memory_active(record_id) VALUES(?1)",
                [record_id],
            )?;
            tx.execute(
                "INSERT INTO memory_history(scope,kind,old_record,new_record) VALUES(?1,?2,?3,?4)",
                params![
                    preview.scope,
                    if old.is_some() { "correct" } else { "remember" },
                    old.map(|(id, _)| id),
                    record_id
                ],
            )?;
        }
        inserted += 1;
    }
    tx.commit()?;
    Ok((inserted, skipped))
}

pub(crate) fn run(cfg: &RuntimeConfig, args: &ImportArgs) -> Result<()> {
    let preview = preview(cfg, args)?;
    let (inserted, skipped) = if args.apply {
        ensure_instance_layout(cfg)?;
        let _lock = cfg.acquire_instance_lock()?;
        let store = Store::open(cfg)?;
        apply(&store, &preview, args.promote_curated)?
    } else {
        (0, 0)
    };
    let excerpts = preview
        .chunks
        .iter()
        .take(8)
        .map(|c| {
            json!({
                "path":c.path, "chunk":c.ordinal, "first_line":c.first, "last_line":c.last,
                "excerpt":c.content.chars().take(80).collect::<String>()
            })
        })
        .collect::<Vec<_>>();
    let mut output = json!({
        "mode":if args.apply {"apply"} else {"preview"}, "scope":preview.scope,
        "files":preview.files, "bytes":preview.bytes, "chunks":preview.chunks.len(),
        "inserted":inserted, "unchanged":skipped,
        "requested_initial_state":if args.promote_curated {"curated"} else {"candidate"},
        "paths":args.files, "previews":if args.apply { Vec::new() } else {excerpts}
    });
    while serde_json::to_string(&output)?.len() > 16384 {
        ensure!(
            output["previews"]
                .as_array_mut()
                .expect("array")
                .pop()
                .is_some(),
            "import summary exceeds output bound"
        );
    }
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

#[cfg(test)]
#[path = "memory_import_tests.rs"]
mod tests;
