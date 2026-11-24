import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

from researcher.db import SCHEMA_VERSION


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
            task_id = connection.execute("INSERT INTO tasks(topic_id,kind,status,profile_id,profile_fingerprint,provider,model,created_at) VALUES(1,'research','success','','','deepseek','deepseek-chat','2026-10-30T00:00:00+00:00')").lastrowid
            connection.execute("INSERT INTO sources(task_id,title,url,content,extent,fetched_at) VALUES(?,?,?,?,?,?)",
                               (task_id, "Hailo-10H AI Accelerator", "https://hailo.ai/products/ai-accelerators/hailo-10h-ai-accelerator/",
                                "Hailo-10H supports edge generative AI applications.", "历史来源摘录", "2026-10-30T00:00:00+00:00"))
        environment = os.environ.copy()
        environment["RESEARCHER_DATA_DIR"] = folder
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        result = subprocess.run([sys.executable, "-m", "researcher.cli", "check"], cwd=root, env=environment, capture_output=True, text=True, timeout=20)
        assert result.returncode == 0, result.stderr
        with sqlite3.connect(database) as connection:
            assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
            assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
            assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
            assert connection.execute("SELECT count(*) FROM topics").fetchone()[0] == 1
            assert connection.execute("SELECT count(*) FROM messages").fetchone()[0] == 1
            assert connection.execute("SELECT body FROM annotations").fetchone()[0] == "希望持续核查软件迁移对产品竞争力的影响。"
            assert connection.execute("SELECT count(*) FROM claims").fetchone()[0] == 0
            assert connection.execute("SELECT auto_enabled FROM station_settings").fetchone()[0] == 0
            assert connection.execute("SELECT count(*) FROM feed_sources").fetchone()[0] == 0
            assert connection.execute("SELECT count(*) FROM sources").fetchone()[0] == 1
            assert connection.execute("SELECT v.content FROM document_task_links l JOIN document_versions v ON v.id=l.recorded_version_id WHERE l.task_id=?", (task_id,)).fetchone()[0] == "Hailo-10H supports edge generative AI applications."
        health_check = """
from fastapi.testclient import TestClient
from researcher.app import app

with TestClient(app, base_url="http://localhost") as client:
    response = client.get("/health")
    response.raise_for_status()
    print(response.text)
"""
        result = subprocess.run([sys.executable, "-c", health_check], cwd=root, env=environment, capture_output=True, text=True, timeout=20)
        assert result.returncode == 0, result.stderr
        health = json.loads(result.stdout)
        assert health["schema_version"] == SCHEMA_VERSION
        assert health["build_id"] and health["started_at"]


def test_source_lifecycle_migration_preserves_existing_ingest_history(tmp_path):
    root = Path(__file__).resolve().parents[1]
    database = tmp_path / "researcher.sqlite3"
    migrations = root / "researcher/migrations"
    now = "2026-11-24T00:00:00+00:00"
    url = "https://hailo.ai/products/ai-accelerators/hailo-10h-ai-accelerator/"
    with sqlite3.connect(database) as connection:
        connection.create_function("sha256", 1, lambda value: hashlib.sha256(value.encode()).hexdigest(), deterministic=True)
        for version in range(1, 9):
            script, = migrations.glob(f"{version:03d}_*.sql")
            connection.executescript(script.read_text())
        connection.execute("PRAGMA user_version=8")
        connection.execute("PRAGMA foreign_keys=ON")
        source_id = connection.execute("INSERT INTO feed_sources(source_key,name,kind,tier,publisher_key,enabled,created_at,updated_at) "
                                       "VALUES('hailo','Hailo','rss','T1','hailo',1,?,?)", (now, now)).lastrowid
        connection.execute("INSERT INTO feed_runs(source_id,started_at,status) VALUES(?,?,'success')", (source_id, now))
        item_id = connection.execute("INSERT INTO feed_items(source_id,canonical_url,original_url,title,discovered_at,tier,publisher_key,updated_at) "
                                     "VALUES(?,?,?,'Hailo-10H AI Accelerator',?,'T1','hailo',?)", (source_id, url, url, now, now)).lastrowid
        connection.execute("INSERT INTO feed_observations(source_id,item_id,seen_at) VALUES(?,?,?)", (source_id, item_id, now))
        connection.execute("INSERT INTO source_checks(feed_source_id,checked_at,final_url,etag) VALUES(?,?,?,'hailo-feed')", (source_id, now, url))
        tables = ("feed_runs", "feed_items", "feed_observations", "source_checks")
        history = {table: connection.execute(f"SELECT * FROM {table}").fetchall() for table in tables}
    environment = dict(os.environ, RESEARCHER_DATA_DIR=str(tmp_path), PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run([sys.executable, "-m", "researcher.cli", "check"], cwd=root, env=environment, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (SCHEMA_VERSION,)
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("SELECT id,enabled,archived_at FROM feed_sources").fetchall() == [(source_id, 1, None)]
        assert connection.execute("SELECT * FROM feed_source_lifecycle").fetchall() == []
        assert {table: connection.execute(f"SELECT * FROM {table}").fetchall() for table in tables} == history


def test_topic_schedule_migration_preserves_history_without_enabling_it(tmp_path):
    root = Path(__file__).resolve().parents[1]
    database = tmp_path / "researcher.sqlite3"
    migrations = root / "researcher/migrations"
    due = "2026-01-05T01:00:00+00:00"
    with sqlite3.connect(database) as connection:
        connection.create_function("sha256", 1, lambda value: hashlib.sha256(value.encode()).hexdigest(), deterministic=True)
        for version in range(1, 10):
            script, = migrations.glob(f"{version:03d}_*.sql")
            connection.executescript(script.read_text())
        connection.execute("PRAGMA user_version=9")
        connection.execute("INSERT INTO topics(title,question,created_at,schedule,schedule_time,weekday,next_run) VALUES(?,?,?,'weekly','09:00',0,?)",
                           ("Hailo 边缘推理", "核对模型支持和功耗。", due, due))
    environment = dict(os.environ, RESEARCHER_DATA_DIR=str(tmp_path), PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run([sys.executable, "-m", "researcher.cli", "check"], cwd=root, env=environment, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (SCHEMA_VERSION,)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("SELECT schedule,schedule_time,weekday,next_run FROM topics").fetchall() == [("weekly", "09:00", 0, due)]
        assert connection.execute("SELECT * FROM topic_research_schedules").fetchall() == []
