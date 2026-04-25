use std::fs;
use std::path::Path;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub fn write_fake_provider_script(
    dir: &Path,
    stem: &str,
    unix_body: String,
    windows_body: String,
) -> String {
    let (path, body, command) = if cfg!(windows) {
        let path = dir.join(format!("{stem}.ps1"));
        let command = format!(
            "powershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"",
            path.display()
        );
        (path, windows_body, command)
    } else {
        let path = dir.join(format!("{stem}.sh"));
        let command = path.display().to_string();
        (path, unix_body, command)
    };

    fs::write(&path, &body).unwrap();

    #[cfg(unix)]
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();

    command
}

pub async fn wait_for_http_ready(base_url: &str) {
    let authority = base_url.trim_start_matches("http://").trim_end_matches('/');
    let mut last_error = String::from("server did not become ready");

    for _ in 0..50 {
        match tokio::net::TcpStream::connect(authority).await {
            Ok(mut stream) => {
                let request = format!(
                    "GET /healthz HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n"
                );
                match stream.write_all(request.as_bytes()).await {
                    Ok(()) => {
                        let mut buf = [0_u8; 256];
                        match tokio::time::timeout(
                            Duration::from_millis(200),
                            stream.read(&mut buf),
                        )
                        .await
                        {
                            Ok(Ok(read)) if read > 0 => {
                                let response = String::from_utf8_lossy(&buf[..read]);
                                if response.starts_with("HTTP/1.1 200")
                                    || response.starts_with("HTTP/1.0 200")
                                {
                                    return;
                                }
                                last_error = format!("unexpected health response: {response}");
                            }
                            Ok(Ok(_)) => {
                                last_error = "health endpoint closed without data".to_string();
                            }
                            Ok(Err(err)) => {
                                last_error = format!("failed reading health response: {err}");
                            }
                            Err(_) => {
                                last_error = "timed out waiting for health response".to_string();
                            }
                        }
                    }
                    Err(err) => {
                        last_error = format!("failed writing health probe: {err}");
                    }
                }
            }
            Err(err) => {
                last_error = format!("failed connecting to health endpoint: {err}");
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!("fake HTTP server did not become ready: {last_error}");
}
