#[cfg(unix)]
mod journal;
#[cfg(unix)]
mod link;
#[cfg(target_os = "linux")]
mod production;
#[cfg(unix)]
mod runner;

#[cfg(unix)]
fn main() -> anyhow::Result<()> {
    runner::run()
}

#[cfg(not(unix))]
fn main() {
    eprintln!("the disposable loopback executor requires Unix");
    std::process::exit(1);
}
