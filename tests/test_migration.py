import os
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path


def test_existing_research_and_observation_migrate_without_becoming_evidence():
    root = Path(__file__).resolve().parents[1]
    work = root / ".work"
    work.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="migration-", dir=work) as folder:
        database = Path(folder) / "researcher.sqlite3"
        with sqlite3.connect(database) as connection:
            connection.executescript((root / "researcher/migrations/001_initial.sql").read_text())
            connection.execute("PRAGMA user_version=1")
            connection.execute("INSERT INTO topics(id,title,question,kind,created_at) VALUES(1,?,?,?,?)", ("独立 AI 加速器领域概览", "关注厂商产品、技术与软件进展、应用场景及市场竞争。", "领域", "2026-10-30T00:00:00+00:00"))
            connection.execute("INSERT INTO messages(topic_id,role,body,created_at) VALUES(1,'observation',?,?)", ("希望持续核查软件迁移对产品竞争力的影响。", "2026-10-30T00:00:00+00:00"))
        environment = os.environ.copy()
        environment["RESEARCHER_DATA_DIR"] = folder
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        result = subprocess.run([sys.executable, "-m", "researcher.cli", "check"], cwd=root, env=environment, capture_output=True, text=True, timeout=20)
        assert result.returncode == 0, result.stderr
        with sqlite3.connect(database) as connection:
            assert connection.execute("PRAGMA user_version").fetchone()[0] == 6
            assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
            assert connection.execute("SELECT count(*) FROM topics").fetchone()[0] == 1
            assert connection.execute("SELECT count(*) FROM messages").fetchone()[0] == 1
            assert connection.execute("SELECT body FROM annotations").fetchone()[0] == "希望持续核查软件迁移对产品竞争力的影响。"
            assert connection.execute("SELECT count(*) FROM claims").fetchone()[0] == 0
            assert connection.execute("SELECT auto_enabled FROM station_settings").fetchone()[0] == 0
