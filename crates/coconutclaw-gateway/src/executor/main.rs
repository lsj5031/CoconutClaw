#[cfg(any(unix, windows))]
mod journal;
#[cfg(any(unix, windows))]
mod link;
#[cfg(target_os = "linux")]
mod production;
#[cfg(any(unix, windows))]
mod runner;
#[cfg(windows)]
mod windows_custody;

#[cfg(any(unix, windows))]
fn main() -> anyhow::Result<()> {
    runner::run()
}

#[cfg(not(any(unix, windows)))]
fn main() {
    eprintln!("the disposable loopback executor requires Unix or Windows");
    std::process::exit(1);
}
