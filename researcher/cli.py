import argparse
import fcntl
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import zipfile
from contextlib import ExitStack
from pathlib import Path

from . import db
from .config import DATA, ROOT, Limits, Profile, config_lock, prepare_data, read_json, write_json


def lock_file(name):
    prepare_data()
    handle = (DATA / name).open("a")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise SystemExit(f"相关服务正在运行（{name}），请停止服务后重试") from None
    return handle


def serve(port):
    import uvicorn
    with lock_file("web.lock"):
        uvicorn.run("researcher.app:app", host="127.0.0.1", port=port, log_level="info", access_log=False)


def run(port):
    children = []

    def stop(signum=None, frame=None):
        for child in children:
            if child.poll() is None:
                child.terminate()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        for command in ([sys.executable, "-m", "researcher.cli", "worker"], [sys.executable, "-m", "researcher.cli", "serve", "--port", str(port)]):
            children.append(subprocess.Popen(command, cwd=ROOT))
        print(f"Researcher Web：http://127.0.0.1:{port}", flush=True)
        while all(child.poll() is None for child in children):
            time.sleep(0.5)
    finally:
        stop()
        for child in children:
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
    failures = [child.returncode for child in children if child.returncode not in (0, -15, 130)]
    if failures:
        raise SystemExit(1)


def backup(destination, include_secrets=False):
    destination = Path(destination).resolve()
    if destination.exists():
        raise SystemExit("备份目标已存在，请选择新文件名")
    db.initialize()
    with config_lock(), tempfile.TemporaryDirectory(prefix=".backup-", dir=DATA) as staging:
        connection = db.connect()
        snapshot_path = Path(staging) / "researcher.sqlite3"
        snapshot = sqlite3.connect(snapshot_path)
        try:
            connection.backup(snapshot)
        finally:
            connection.close()
            snapshot.close()
        descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle, zipfile.ZipFile(handle, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.write(snapshot_path, "researcher.sqlite3")
            archive.writestr("config.json", json.dumps(read_json("config.json", {}), ensure_ascii=False))
            archive.writestr("manifest.json", json.dumps({"version": 2, "created_at": db.now(), "includes_secrets": include_secrets}))
            if include_secrets:
                archive.writestr("secrets.json", json.dumps(read_json("secrets.json", {})))
    print(f"备份已保存：{destination}；{'包含密钥，请妥善保管' if include_secrets else '不包含 API 密钥'}")


def restore(source, replace=False):
    prepare_data()
    with ExitStack() as stack:
        stack.enter_context(lock_file("web.lock"))
        stack.enter_context(lock_file("worker.lock"))
        stack.enter_context(config_lock())
        staging = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix=".restore-", dir=DATA)))
        destination = DATA / "researcher.sqlite3"
        if destination.exists() and not replace:
            raise SystemExit("数据目录已有数据库；确认替换时请使用 --replace，并先备份")
        with zipfile.ZipFile(source) as archive:
            if sum(info.file_size for info in archive.infolist()) > 512_000_000:
                raise SystemExit("备份超过 512 MB 的恢复限制")
            manifest = json.loads(archive.read("manifest.json"))
            if manifest["version"] not in (1, 2):
                raise SystemExit("备份版本不受支持")
            contents = archive.read("researcher.sqlite3")
            config = json.loads(archive.read("config.json"))
            Limits(**config.get("limits", {}))
            for item in config.get("profiles", []):
                Profile(**item)
                item["chat_verified"] = None
                item["search_verified"] = None
            keys = json.loads(archive.read("secrets.json")) if manifest.get("includes_secrets") else None
        snapshot_path = staging / "researcher.sqlite3"
        snapshot_path.write_bytes(contents)
        snapshot = sqlite3.connect(snapshot_path)
        try:
            version = snapshot.execute("PRAGMA user_version").fetchone()[0]
            if snapshot.execute("PRAGMA integrity_check").fetchone()[0] != "ok" or version not in range(1, db.SCHEMA_VERSION + 1):
                raise SystemExit("备份数据库完整性或版本检查未通过")
            if snapshot.execute("PRAGMA foreign_key_check").fetchall():
                raise SystemExit("备份数据库关联检查未通过")
            snapshot.execute("UPDATE tasks SET status='interrupted',error='从备份恢复，任务没有自动重试' WHERE status IN ('queued','running')")
            snapshot.execute("UPDATE topics SET schedule='manual',next_run=NULL")
            if version >= 2:
                snapshot.execute("UPDATE station_settings SET auto_enabled=0,next_run=NULL,last_status='never',last_error='' WHERE id=1")
            snapshot.execute("DELETE FROM runtime")
            snapshot.commit()
            target = sqlite3.connect(destination)
            try:
                snapshot.backup(target)
            finally:
                target.close()
        finally:
            snapshot.close()
        destination.chmod(0o600)
        write_json("config.json", config)
        if keys is not None:
            write_json("secrets.json", keys)
    db.initialize()
    print("数据已恢复。自动研究已暂停，请检查模型密钥、重新测试连接并确认调度。")


def install_services(port):
    directory = Path.home() / ".config/systemd/user"
    directory.mkdir(parents=True, exist_ok=True)
    for name, command, description in (
        ("researcher-web", f"serve --port {port}", "Researcher Web 本机网站"),
        ("researcher-worker", "worker", "Researcher Web 后台研究服务"),
    ):
        unit = f"""[Unit]
Description={description}
After=network-online.target

[Service]
Type=simple
WorkingDirectory={ROOT}
ExecStart={sys.executable} -m researcher.cli {command}
Environment=RESEARCHER_DATA_DIR={DATA}
Environment=PYTHONUNBUFFERED=1
Environment=PYTHONDONTWRITEBYTECODE=1
UMask=0077
Restart=on-failure
RestartSec=5
TimeoutStopSec=20

[Install]
WantedBy=default.target
"""
        (directory / f"{name}.service").write_text(unit)
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=True)
    print("服务配置已安装。启用：systemctl --user enable --now researcher-web researcher-worker")


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description="Researcher Web 本机研究工作台")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("run", "serve", "install-services"):
        sub = commands.add_parser(command)
        sub.add_argument("--port", type=int, default=8765)
    commands.add_parser("worker")
    commands.add_parser("check")
    sub = commands.add_parser("backup")
    sub.add_argument("destination")
    sub.add_argument("--include-secrets", action="store_true")
    sub = commands.add_parser("restore")
    sub.add_argument("source")
    sub.add_argument("--replace", action="store_true")
    arguments = parser.parse_args()
    if arguments.command == "run":
        run(arguments.port)
    elif arguments.command == "serve":
        serve(arguments.port)
    elif arguments.command == "worker":
        from .worker import main as worker
        worker()
    elif arguments.command == "install-services":
        install_services(arguments.port)
    elif arguments.command == "backup":
        backup(arguments.destination, arguments.include_secrets)
    elif arguments.command == "restore":
        restore(arguments.source, arguments.replace)
    elif arguments.command == "check":
        db.initialize()
        with db.connect() as connection:
            result = connection.execute("PRAGMA integrity_check").fetchone()[0]
            version = connection.execute("PRAGMA user_version").fetchone()[0]
        print(f"数据库完整性：{result}；版本：{version}；数据目录：{DATA}")
        if result != "ok":
            raise SystemExit(1)


if __name__ == "__main__":
    main()
