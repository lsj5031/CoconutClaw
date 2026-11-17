fn main() {
    println!("cargo:rerun-if-env-changed=COCONUTCLAW_SOURCE_REVISION");
    let git = |args: &[&str]| {
        std::process::Command::new("git")
            .args(args)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .map(|output| output.trim().to_string())
    };
    let mut refs = vec!["HEAD".to_string()];
    if let Some(branch) = git(&["symbolic-ref", "-q", "HEAD"]) {
        refs.push(branch);
    }
    for name in refs {
        if let Some(path) = git(&["rev-parse", "--path-format=absolute", "--git-path", &name]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    let revision = std::env::var("COCONUTCLAW_SOURCE_REVISION")
        .ok()
        .or_else(|| git(&["rev-parse", "HEAD"]))
        .unwrap_or_else(|| "unavailable".into());
    println!("cargo:rustc-env=COCONUTCLAW_REVISION={}", revision.trim());
}
