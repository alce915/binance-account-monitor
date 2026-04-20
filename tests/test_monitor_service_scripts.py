from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[1]
STOP_SCRIPT = PROJECT_ROOT / "scripts" / "stop_monitor_service.ps1"
COMMON_SCRIPT = PROJECT_ROOT / "scripts" / "monitor_service_common.ps1"
SYSTEM_ROOT = Path(os.environ.get("SystemRoot") or r"C:\Windows")
POWERSHELL_EXE = str(SYSTEM_ROOT / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe")
TASKLIST_EXE = str(SYSTEM_ROOT / "System32" / "tasklist.exe")
TASKKILL_EXE = str(SYSTEM_ROOT / "System32" / "taskkill.exe")


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_health(port: int, timeout_s: float = 10.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urlopen(f"http://127.0.0.1:{port}/healthz", timeout=1) as response:
                if response.status == 200:
                    return True
        except (OSError, URLError):
            time.sleep(0.2)
    return False


def _process_exists(pid: int) -> bool:
    result = subprocess.run(
        [TASKLIST_EXE, "/FI", f"PID eq {pid}"],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    return str(pid) in result.stdout


def _run_stop_script(env: dict[str, str], log_path: Path) -> subprocess.CompletedProcess[str]:
    with log_path.open("w", encoding="utf-8", errors="ignore") as stream:
        return subprocess.run(
            [
                POWERSHELL_EXE,
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(STOP_SCRIPT),
            ],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=stream,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
            timeout=120,
        )


def test_monitor_health_check_returns_false_without_terminating_on_connection_refused(tmp_path: Path) -> None:
    port = _find_free_port()
    script = (
        f". '{COMMON_SCRIPT}'; "
        f"$healthy = Test-ServiceHealth -HostAddress '127.0.0.1' -Port {port}; "
        "if ($healthy) { Write-Output 'HEALTHY' } else { Write-Output 'UNHEALTHY' }"
    )
    result = subprocess.run(
        [
            POWERSHELL_EXE,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            script,
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout.strip().endswith("UNHEALTHY"), result.stdout


def test_stop_monitor_service_kills_tracked_process_tree_and_clears_state(tmp_path: Path) -> None:
    port = _find_free_port()
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()

    child_pid_file = tmp_path / "child.pid"
    server_script = tmp_path / "dummy_health_server.py"
    server_script.write_text(
        "\n".join(
            [
                "from http.server import BaseHTTPRequestHandler, HTTPServer",
                "import sys",
                "class Handler(BaseHTTPRequestHandler):",
                "    def do_GET(self):",
                "        if self.path == '/healthz':",
                "            self.send_response(200)",
                "            self.end_headers()",
                "            self.wfile.write(b'{\"status\":\"ok\"}')",
                "            return",
                "        self.send_response(200)",
                "        self.end_headers()",
                "        self.wfile.write(b'ok')",
                "    def log_message(self, format, *args):",
                "        return",
                "port = int(sys.argv[1])",
                "HTTPServer(('127.0.0.1', port), Handler).serve_forever()",
            ]
        ),
        encoding="utf-8",
    )

    wrapper_command = (
        f"$p = Start-Process -FilePath '{sys.executable}' "
        f"-ArgumentList '{server_script}','{port}' -PassThru; "
        f"Set-Content -Path '{child_pid_file}' -Value $p.Id; "
        "Wait-Process -Id $p.Id"
    )
    wrapper = subprocess.Popen(
        [
            POWERSHELL_EXE,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            wrapper_command,
        ],
        cwd=PROJECT_ROOT,
        )

    child_pid: int | None = None
    try:
        assert _wait_for_health(port), "dummy health server did not start in time"
        child_pid = int(child_pid_file.read_text(encoding="utf-8").strip())

        state_path = runtime_dir / "monitor-service.state.json"
        state_path.write_text(
            json.dumps(
                {
                    "host": "127.0.0.1",
                    "port": port,
                    "launcher_pid": wrapper.pid,
                    "service_pid": child_pid,
                    "listener_pids": [child_pid],
                    "started_at": "2026-04-18T00:00:00+08:00",
                }
            ),
            encoding="utf-8",
        )

        env = os.environ.copy()
        env.update(
            {
                "MONITOR_SCRIPT_SKIP_ELEVATION": "1",
                "MONITOR_API_HOST_OVERRIDE": "127.0.0.1",
                "MONITOR_API_PORT_OVERRIDE": str(port),
                "MONITOR_RUNTIME_DIR": str(runtime_dir),
            }
        )

        log_path = tmp_path / "stop-script.log"
        result = _run_stop_script(env, log_path)
        output = log_path.read_text(encoding="utf-8", errors="ignore")

        assert result.returncode == 0, output
        assert not _wait_for_health(port, timeout_s=2.0), "health endpoint still reachable after stop"
        assert not _process_exists(child_pid), f"listener process {child_pid} still exists"
        wrapper.wait(timeout=10)
        assert not _process_exists(wrapper.pid), f"launcher process {wrapper.pid} still exists"
        assert not state_path.exists(), "runtime state file should be cleared after stop"
    finally:
        if wrapper.poll() is None:
            wrapper.kill()
        if child_pid and _process_exists(child_pid):
            subprocess.run(
                [TASKKILL_EXE, "/PID", str(child_pid), "/F", "/T"],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
