//! Locate the normal runtime for unit tests that exercise a separate process.
//! Cargo's test-artifact directory depth is not a stable interface.
use std::path::{Path, PathBuf};

pub(crate) fn binary() -> PathBuf {
    locate(
        &std::env::current_exe().expect("current test executable"),
        std::env::var_os("COCONUTCLAW_TEST_BINARY").as_deref().map(Path::new),
    )
    .expect("build the normal runtime first with cargo build -p coconutclaw --bin coconutclaw, or set COCONUTCLAW_TEST_BINARY")
}

fn locate(test: &Path, selected: Option<&Path>) -> Option<PathBuf> {
    if let Some(selected) = selected {
        return selected.is_file().then(|| selected.to_path_buf());
    }
    let name = format!("coconutclaw{}", std::env::consts::EXE_SUFFIX);
    test.parent()?
        .ancestors()
        .map(|directory| directory.join(&name))
        .find(|candidate| candidate != test && candidate.is_file())
}

#[test]
fn normal_runtime_lookup_supports_cargo_artifact_layouts_and_explicit_override() {
    let root = tempfile::tempdir().unwrap();
    let profile = root.path().join("debug");
    let deep = profile.join("build/coconutclaw/hash/out");
    std::fs::create_dir_all(&deep).unwrap();
    std::fs::create_dir(profile.join("deps")).unwrap();
    let normal = profile.join(format!("coconutclaw{}", std::env::consts::EXE_SUFFIX));
    std::fs::write(&normal, b"normal-runtime-fixture").unwrap();
    assert_eq!(
        locate(&profile.join("deps/coconutclaw-hash"), None),
        Some(normal.clone())
    );
    assert_eq!(
        locate(&deep.join("coconutclaw-hash"), None),
        Some(normal.clone())
    );
    let explicit = root.path().join("selected-runtime");
    std::fs::write(&explicit, b"explicit-runtime-fixture").unwrap();
    assert_eq!(
        locate(&deep.join("coconutclaw-hash"), Some(&explicit)),
        Some(explicit)
    );
    // A bad explicit selection must fail, not silently execute another binary.
    assert_eq!(
        locate(
            &deep.join("coconutclaw-hash"),
            Some(&root.path().join("missing"))
        ),
        None
    );
}
