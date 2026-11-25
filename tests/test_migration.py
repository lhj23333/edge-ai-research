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
        projections = {table: ",".join(row[1] for row in connection.execute(f"PRAGMA table_info({table})")) for table in tables}
        history = {table: connection.execute(f"SELECT {projections[table]} FROM {table}").fetchall() for table in tables}
    environment = dict(os.environ, RESEARCHER_DATA_DIR=str(tmp_path), PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run([sys.executable, "-m", "researcher.cli", "check"], cwd=root, env=environment, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (SCHEMA_VERSION,)
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("SELECT id,enabled,archived_at FROM feed_sources").fetchall() == [(source_id, 1, None)]
        assert connection.execute("SELECT * FROM feed_source_lifecycle").fetchall() == []
        assert {table: connection.execute(f"SELECT {projections[table]} FROM {table}").fetchall() for table in tables} == history
        assert connection.execute("SELECT manual_release,manual_target_event_id FROM feed_items").fetchall() == [(0, None)]


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


def test_migration_011_consolidates_existing_model_aliases(tmp_path):
    root = Path(__file__).resolve().parents[1]
    database = tmp_path / "researcher.sqlite3"
    migrations = root / "researcher/migrations"
    created = "2026-11-24T00:00:00+00:00"
    with sqlite3.connect(database) as connection:
        connection.create_function("sha256", 1, lambda value: hashlib.sha256(value.encode()).hexdigest(), deterministic=True)
        for version in range(1, 11):
            script, = migrations.glob(f"{version:03d}_*.sql")
            connection.executescript(script.read_text())
        connection.execute("PRAGMA user_version=10")
        names = ("Ara240", "Ara240 DNPU", "Hailo-8", "Hailo-10H", "Hailo 10H")
        connection.executemany("INSERT INTO entities(kind,name,normalized_name,state,created_at,updated_at) VALUES('产品',?,?,'active',?,?)",
                               ((name, name.casefold(), created, created) for name in names))
        connection.execute("INSERT INTO entity_fields(entity_id,field_name,value,updated_at) VALUES(2,'性能','40 eTOPS',?)", (created,))
    environment = dict(os.environ, RESEARCHER_DATA_DIR=str(tmp_path), PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run([sys.executable, "-m", "researcher.cli", "check"], cwd=root, env=environment, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (SCHEMA_VERSION,)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("SELECT name FROM entities WHERE canonical_entity_id IS NULL ORDER BY id").fetchall() == [("Ara240",), ("Hailo-8",), ("Hailo-10H",)]
        assert connection.execute("SELECT name,entity_id FROM entity_aliases ORDER BY entity_id").fetchall() == [("Ara240 DNPU", 1), ("Hailo 10H", 4)]
        assert connection.execute("SELECT entity_id,value FROM entity_fields").fetchall() == [(1, "40 eTOPS")]


def test_migration_012_keeps_history_and_web_pages_render(tmp_path):
    root = Path(__file__).resolve().parents[1]
    migrations = root / "researcher/migrations"
    database = tmp_path / "researcher.sqlite3"
    created = "2026-11-24T00:00:00+00:00"
    with sqlite3.connect(database) as connection:
        connection.create_function("sha256", 1, lambda value: hashlib.sha256(value.encode()).hexdigest(), deterministic=True)
        for version in range(1, 12):
            script, = migrations.glob(f"{version:03d}_*.sql")
            connection.executescript(script.read_text())
        connection.execute("PRAGMA user_version=11")
        connection.execute("PRAGMA foreign_keys=ON")
        topic = connection.execute("INSERT INTO topics(title,question,created_at) VALUES(?,?,?)",
                                   ("Hailo 边缘推理", "核对模型支持和功耗。", created)).lastrowid
        session = connection.execute("INSERT INTO research_sessions(title,purpose,scope_json,status,created_at,updated_at) "
                                     "VALUES('Hailo-10H 部署条件','部署决策','{}','partial',?,?)", (created, created)).lastrowid
        task = connection.execute("INSERT INTO tasks(kind,status,profile_id,profile_fingerprint,provider,model,created_at,"
                                  "topic_id,session_id,automatic,schedule_key,result_json) "
                                  "VALUES('revise','success','glm','','zhipu','GLM-5.2',?,?,?,1,?,?)",
                                  (created, topic, session, f"topic-revision:{topic}:{created}",
                                   json.dumps({"knowledge_ids": None, "degraded_reason": None}))).lastrowid
        connection.executemany("INSERT INTO usage(task_id,profile_id,provider,model,operation,created_at,day,status,input_tokens,output_tokens,estimate,reserved_estimate) "
                               "VALUES(?,'glm','zhipu','GLM-5.2','synthesis',?,'2026-11-24',?,?,?,?,?)",
                               ((task, created, "success", 500, 120, 0.015, 0.025),
                                (task, created, "reserved", None, None, None, 0.025),
                                (task, created, "interrupted", None, None, None, None)))
        connection.execute("INSERT INTO daily_briefings(day,created_at,event_ids_json,background_ids_json,revision_ids_json) "
                           "VALUES('2026-11-24',?,'[]','[]','[]')", (created,))
        connection.execute("INSERT INTO knowledge_items(identity_key,kind,body,status,created_at,updated_at) "
                           "VALUES('hailo-10h','公开披露','Hailo-10H 支持边缘生成式 AI 推理。','supported',?,?)", (created, created))

    health_check = f"""
from fastapi.testclient import TestClient
from researcher.app import app

with TestClient(app, base_url="http://localhost") as client:
    assert client.get("/health").json()["schema_version"] == {SCHEMA_VERSION}
    for path in ("/", "/settings", "/research", "/research/{session}", "/admin/eval"):
        response = client.get(path)
        assert response.status_code == 200, response.text
    assert "Hailo-10H 部署条件" in client.get("/research/{session}").text
"""
    environment = dict(os.environ, RESEARCHER_DATA_DIR=str(tmp_path), PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run([sys.executable, "-c", health_check], cwd=root, env=environment, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (12,)
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("SELECT request_key,attempt_count,estimated_cost,tokens,auto_released_at FROM model_call_ledger ORDER BY id").fetchall() == [
            (None, 1, 0.015, 620, None), (None, 2, 0.025, None, "2026-11-24T00:12:00+00:00"), (None, 3, None, None, None),
        ]
        assert connection.execute("SELECT task_id,topic_id,attempt_count,status FROM topic_retry_quotas").fetchall() == [(task, topic, 3, "finished")]
        assert connection.execute("SELECT body,polished,evidence_hash,polish_attempted FROM daily_briefings").fetchall() == [("", "", "", None)]
        assert connection.execute("SELECT evidence_hash FROM knowledge_items").fetchall() == [(None,)]
        assert connection.execute("SELECT * FROM content_cache").fetchall() == []
        assert connection.execute("SELECT * FROM research_evidence_versions").fetchall() == []
