use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::thread;

/// A single log line from the subprocess.
#[derive(Clone, Debug)]
pub struct LogLine {
    pub text: String,
    pub is_stderr: bool,
}

/// Handle to a running Python subprocess.
pub struct RunHandle {
    pub rx: mpsc::Receiver<LogLine>,
    pub join: Option<thread::JoinHandle<Option<ExitStatus>>>,
}

impl RunHandle {
    /// Check if the subprocess has finished. Returns `Some(status)` if done.
    pub fn try_finish(&mut self) -> Option<ExitStatus> {
        if self.join.as_ref().map_or(true, |j| j.is_finished()) {
            self.join.take().and_then(|j| j.join().ok().flatten())
        } else {
            None
        }
    }
}

/// Locate the project root (parent of podcast-studio/).
fn project_root() -> std::path::PathBuf {
    let exe = std::env::current_exe().unwrap_or_default();
    // During development, exe is in target/debug or target/release
    // Walk up until we find run.py
    let mut dir = exe.parent().map(|p| p.to_path_buf()).unwrap_or_default();
    for _ in 0..10 {
        if dir.join("run.py").exists() {
            return dir;
        }
        if let Some(parent) = dir.parent() {
            dir = parent.to_path_buf();
        } else {
            break;
        }
    }
    // Fallback: current working directory
    std::env::current_dir().unwrap_or_default()
}

/// Spawn a Python command in the background, streaming stdout/stderr to a channel.
pub fn spawn_python(args: &[&str]) -> RunHandle {
    let root = project_root();
    let args_owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let (tx, rx) = mpsc::channel();

    let join = thread::spawn(move || {
        let mut cmd = Command::new("python");
        cmd.arg(root.join("run.py"))
            .args(&args_owned)
            .current_dir(&root)
            .env("PYTHONUNBUFFERED", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                let _ = tx.send(LogLine {
                    text: format!("Failed to spawn Python: {e}"),
                    is_stderr: true,
                });
                return None;
            }
        };

        // Read stdout in a thread
        let stdout = child.stdout.take();
        let tx_out = tx.clone();
        let stdout_thread = thread::spawn(move || {
            if let Some(out) = stdout {
                for line in BufReader::new(out).lines() {
                    if let Ok(line) = line {
                        let _ = tx_out.send(LogLine { text: line, is_stderr: false });
                    }
                }
            }
        });

        // Read stderr in a thread
        let stderr = child.stderr.take();
        let tx_err = tx.clone();
        let stderr_thread = thread::spawn(move || {
            if let Some(err) = stderr {
                for line in BufReader::new(err).lines() {
                    if let Ok(line) = line {
                        let _ = tx_err.send(LogLine { text: line, is_stderr: true });
                    }
                }
            }
        });

        let status = child.wait().ok();
        let _ = stdout_thread.join();
        let _ = stderr_thread.join();
        status
    });

    RunHandle {
        rx,
        join: Some(join),
    }
}

/// Open a file in the system default editor.
pub fn open_in_editor(path: &Path) {
    #[cfg(target_os = "windows")]
    {
        let _ = Command::new("explorer").arg(path).spawn();
    }
    #[cfg(target_os = "macos")]
    {
        let _ = Command::new("open").arg(path).spawn();
    }
    #[cfg(target_os = "linux")]
    {
        let _ = Command::new("xdg-open").arg(path).spawn();
    }
}

/// Open a file specifically in VS Code.
pub fn open_in_vscode(path: &Path) {
    #[cfg(target_os = "windows")]
    {
        // On Windows, try "code.cmd" first (installed via PATH), then "code"
        if Command::new("code.cmd").arg(path).spawn().is_err() {
            let _ = Command::new("code").arg(path).spawn();
        }
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = Command::new("code").arg(path).spawn();
    }
}
