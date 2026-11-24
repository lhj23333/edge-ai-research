import json
import secrets
import sqlite3
import os
import subprocess
import sys
import time
import zipfile
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit

import pytest
from pydantic import ValidationError

from researcher import db
from researcher.app import BUILD_ID, render_markdown
from researcher.cli import backup, lock_file, restore
from researcher.config import DATA, Profile, save_profile, settings
from researcher.research_engine import ResearchRun
from researcher.providers import source_url
from researcher.scheduler import next_run, next_topic_run, tick
from researcher.service import save_artifact
from researcher.providers import structured_json


@pytest.mark.parametrize("path", ["/", "/updates", "/map", "/map/data", "/companies", "/compare", "/evidence", "/questions", "/personal", "/watchlist", "/topics", "/library", "/settings", "/tasks", "/health", "/static/style.css", "/static/station.css", "/static/station.js", "/static/app.js", "/static/htmx.min.js"])
def test_routes_and_headers(client, path):
    response = client.get(path)
    assert response.status_code == 200
    assert response.headers["x-frame-options"] == "DENY"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]


def test_health_exposes_loaded_build_and_database_schema(client):
    response = client.get("/health")
    assert response.status_code == 200
    info = response.json()
    assert set(info) == {"status", "version", "worker", "build_id", "started_at", "schema_version"}
    assert info["status"] == "ok"
    assert info["build_id"] == BUILD_ID and len(BUILD_ID) == 12
    assert datetime.fromisoformat(info["started_at"]).tzinfo == timezone.utc
    assert info["schema_version"] == db.one("PRAGMA user_version")["user_version"] == db.SCHEMA_VERSION


@pytest.mark.parametrize("invalid", ["9223372036854775808", "0", "-1", "invalid"])
def test_record_ids_are_rejected_before_querying_sqlite(client, csrf, topic, invalid):
    paths = (
        ("GET", f"/topics/{invalid}"),
        ("POST", f"/topics/{invalid}/tasks"),
        ("POST", f"/topics/{invalid}/schedule"),
        ("POST", f"/events/{invalid}/research"),
        ("POST", f"/research/gaps/{invalid}/pause"),
        ("GET", f"/documents/1/snapshots/{invalid}"),
        ("GET", f"/updates?topic_id={invalid}"),
        ("GET", f"/questions?claim_id={invalid}"),
        ("GET", f"/admin/sources?edit={invalid}"),
        ("GET", f"/admin/funnel?item_id={invalid}"),
        ("POST", f"/admin/sources/{invalid}/push"),
        ("GET", f"/compare?id={topic['id']}&id={invalid}"),
    )
    for method, path in paths:
        response = client.request(method, path, data={"csrf": csrf} if method == "POST" else None)
        assert response.status_code == 422, (method, path, response.text)


def test_empty_id_filters_and_question_action_links_remain_usable(client, topic):
    assert client.get("/updates?topic_id=").status_code == 200
    assert client.get("/admin/funnel?source_id=").status_code == 200
    response = client.get("/questions", params={"target_id": topic["id"]})
    assert response.status_code == 200
    assert f'target_id={topic["id"]}&action=verify' in response.text
    assert "event_id=None" not in response.text and "claim_id=None" not in response.text
    assert client.get(f"/questions?target_id={topic['id']}&action=verify").status_code == 200
    assert client.get("/compare?id=9223372036854775807").status_code == 200


def test_create_edit_observation_and_context(client, csrf, topic):
    path = f"/topics/{topic['id']}"
    assert client.get(path).status_code == 200
    assert client.post(path + "/edit", data={"csrf": csrf, "title": topic["title"], "question": "重点关注模型支持与部署条件。"}).status_code == 200
    observation = "Demo 希望通过阅读和讨论逐步积累研究成果。"
    assert client.post(path + "/observations", data={"csrf": csrf, "body": observation}).status_code == 200
    assert not db.query("SELECT id FROM claims WHERE statement=?", (observation,))
    assert db.one("SELECT body FROM annotations WHERE target_kind='topic' AND target_id=?", (topic["id"],))["body"] == observation
    assert db.one("SELECT * FROM topics WHERE id=?", (topic["id"],))["question"] == "重点关注模型支持与部署条件。"


def test_csrf_and_origin_validation(client, topic):
    path = f"/topics/{topic['id']}/observations"
    assert client.post(path, data={"body": "研究观察"}).status_code == 403
    assert client.post(path, data={"csrf": client.cookies["researcher_csrf"], "body": "研究观察"}, headers={"origin": "https://example.com"}).status_code == 403
    assert client.get("/", headers={"host": "external.example"}).status_code == 400
    assert not db.query("SELECT * FROM messages")


def test_no_key_blocks_research_and_automation(client, csrf, topic):
    response = client.post("/updates/collect", data={"csrf": csrf})
    assert "请在信源管理中启用采集源" in response.text
    assert client.post("/updates/scan", data={"csrf": csrf}).status_code == 404
    response = client.post("/watchlist/automation", data={"csrf": csrf, "enabled": "yes", "schedule_time": "09:00"})
    assert "请先在信源管理中启用信源" in response.text
    assert db.one("SELECT schedule FROM topics WHERE id=?", (topic["id"],))["schedule"] == "manual"
    assert not db.query("SELECT * FROM tasks")


def test_historical_topic_schedule_stays_parked_until_explicitly_enabled(client, csrf, topic):
    identifier = topic["id"]
    due = "2026-01-05T01:00:00+00:00"
    db.execute("UPDATE topics SET schedule='weekly',schedule_time='09:00',weekday=0,next_run=? WHERE id=?", (due, identifier))
    profile = Profile(id="glm", name="智谱", provider="zhipu", model="GLM-5.2", base_url="https://open.bigmodel.cn/api/paas/v4",
                      chat_verified=db.now(), search_verified=db.now())
    save_profile(profile, secrets.token_urlsafe())
    db.execute("UPDATE station_settings SET auto_enabled=1,watchlist_confirmed=1,sources_confirmed=1 WHERE id=1")
    path = f"/topics/{identifier}"
    assert "历史定时计划已停用" in client.get(path).text
    assert client.post(path + "/schedule", data={"schedule": "weekly"}).status_code == 403
    tick()
    assert not db.query("SELECT id FROM tasks")

    fields = {"csrf": csrf, "schedule": "weekly", "schedule_time": "09:00", "weekday": 0}
    response = client.post(path + "/schedule", data={**fields, "action": "save"})
    assert "专题定向更新计划已保存" in response.text
    assert db.one("SELECT next_run FROM topic_research_schedules")["next_run"] is None
    response = client.post(path + "/schedule", data=fields)
    assert "专题定向更新计划已启用" in response.text
    run_at = datetime.fromisoformat(db.one("SELECT next_run FROM topic_research_schedules")["next_run"])
    assert run_at > datetime.now(timezone.utc) and run_at.weekday() == 0 and run_at.hour == 1

    db.execute("UPDATE topic_research_schedules SET next_run=?", (due,))
    tick()
    tick()
    tasks = db.query("SELECT * FROM tasks")
    assert len(tasks) == 1
    task = tasks[0]
    assert task["kind"] == "revise" and task["automatic"] and task["topic_id"] == identifier
    assert task["schedule_key"] == f"topic-revision:{identifier}:{due}"
    run = ResearchRun(task)
    assert not run.can_search and not run.can_read
    assert run.base_context()["target"]["id"] == identifier
    assert db.one("SELECT next_run FROM topic_research_schedules")["next_run"] > due
    response = client.post(path + "/schedule", data={"csrf": csrf, "schedule": "manual"})
    assert "专题定向更新计划已停用" in response.text
    assert db.one("SELECT frequency,next_run FROM topic_research_schedules") == {"frequency": "weekly", "next_run": None}
    assert db.one("SELECT status FROM tasks")["status"] == "cancelled"
    assert db.one("SELECT schedule,next_run FROM topics WHERE id=?", (identifier,)) == {"schedule": "weekly", "next_run": due}
    tick()
    assert db.query("SELECT kind FROM tasks") == [{"kind": "revise"}]
    assert "专题定向更新计划已启用" in client.post(path + "/schedule", data={**fields, "action": "enable"}).text
    assert client.post(f"/knowledge/{identifier}/archive", data={"csrf": csrf}, follow_redirects=False).status_code == 303
    assert db.one("SELECT next_run FROM topic_research_schedules")["next_run"] is None


def test_artifact_version_search_edit_and_export(client, csrf, topic):
    first_id = save_artifact(topic["id"], "Researcher Web 研究意图", topic["question"])
    first = db.one("SELECT * FROM artifacts WHERE id=?", (first_id,))
    assert client.get(f"/artifacts/{first_id}").status_code == 200
    changed = topic["question"] + "\n\n保留重要信息的来源与尚待确认的问题。"
    response = client.post(f"/artifacts/{first_id}", data={"csrf": csrf, "title": first["title"], "body": changed})
    assert response.status_code == 200
    latest = db.one("SELECT * FROM artifacts ORDER BY id DESC LIMIT 1")
    assert latest["version"] == 2
    assert latest["parent_id"] == first_id
    assert db.one("SELECT body FROM artifacts WHERE id=?", (first_id,))["body"] == topic["question"]
    assert first["title"] in client.get("/library", params={"q": "Researcher"}).text
    response = client.get(f"/artifacts/{latest['id']}/export")
    assert "attachment" in response.headers["content-disposition"]
    assert changed in response.text
    assert latest["body"] == changed


def test_limits_persist_and_reject_invalid_values(client, csrf):
    client.post("/settings/limits", data={"csrf": csrf, "daily_requests": 20, "task_requests": 5, "output_tokens": 2048})
    assert settings()["limits"]["daily_requests"] == 20
    client.post("/settings/limits", data={"csrf": csrf, "daily_requests": -1, "task_requests": 0, "output_tokens": 0})
    assert settings()["limits"]["daily_requests"] == 20
    assert (DATA / "config.json").stat().st_mode & 0o777 == 0o600
    assert DATA.stat().st_mode & 0o777 == 0o700


@pytest.mark.parametrize("invalid", ["Asia/Invalid_Audit_Zone", "", "   "])
def test_limits_reject_invalid_timezone_without_saving_any_changes(client, csrf, invalid):
    valid = client.post("/settings/limits", data={"csrf": csrf, "timezone": "UTC", "daily_requests": "47"}, follow_redirects=False)
    assert valid.status_code == 303 and "notice=" in valid.headers["location"]
    before = (DATA / "config.json").read_bytes()
    response = client.post("/settings/limits", data={"csrf": csrf, "timezone": invalid, "daily_requests": "48"}, follow_redirects=False)
    assert response.status_code == 303
    location = urlsplit(response.headers["location"])
    assert location.path == "/settings"
    assert "时区" in parse_qs(location.query)["error"][0]
    assert (DATA / "config.json").read_bytes() == before
    assert settings()["limits"]["timezone"] == "UTC"


def test_profiles_reject_missing_keys_and_invalid_address(client, csrf):
    response = client.post("/settings/profiles", data={"csrf": csrf, "name": "通义千问", "provider": "qwen", "base_url": "https://dashscope.aliyuncs.com/api/v1", "model": "qwen-plus"})
    assert "首次保存配置时需要填写 API 密钥" in response.text
    assert not settings()["profiles"]
    with pytest.raises(ValidationError):
        Profile(id="qwen", name="通义千问", provider="qwen", base_url="http://127.0.0.1", model="qwen-plus")


def test_real_calendar_boundaries():
    friday = datetime(2026, 10, 30, 1, 0, tzinfo=timezone.utc)
    assert next_run("09:00", "Asia/Shanghai", friday) == "2026-10-31T01:00:00+00:00"
    assert next_run("09:01", "Asia/Shanghai", friday) == "2026-10-30T01:01:00+00:00"
    assert next_topic_run("weekly", "09:00", 4, "Asia/Shanghai", friday) == "2026-11-06T01:00:00+00:00"
    assert next_topic_run("weekly", "09:00", 0, "Asia/Shanghai", friday) == "2026-11-02T01:00:00+00:00"


def test_backup_restore_and_running_service_guard(topic):
    first = save_artifact(topic["id"], "项目研究意图", topic["question"])
    db.execute("INSERT INTO topic_research_schedules(topic_id,frequency,schedule_time,weekday,next_run) VALUES(?,'weekly','09:00',0,?)",
               (topic["id"], db.now()))
    destination = DATA.parent / "roundtrip.zip"
    if destination.exists():
        destination.unlink()
    backup(destination)
    with zipfile.ZipFile(destination) as archive:
        assert "secrets.json" not in archive.namelist()
        assert json.loads(archive.read("manifest.json"))["version"] == 2
    with pytest.raises(SystemExit):
        restore(destination)
    with lock_file("worker.lock"):
        with pytest.raises(SystemExit):
            restore(destination, replace=True)
    db.execute("UPDATE artifacts SET title='研究意图补充' WHERE id=?", (first,))
    restore(destination, replace=True)
    assert db.one("SELECT title FROM artifacts WHERE id=?", (first,))["title"] == "项目研究意图"
    assert db.one("SELECT schedule FROM topics WHERE id=?", (topic["id"],))["schedule"] == "manual"
    assert db.one("SELECT frequency,next_run FROM topic_research_schedules") == {"frequency": "weekly", "next_run": None}
    assert not list(DATA.glob(".backup-*"))
    assert not list(DATA.glob(".restore-*"))
    destination.unlink()


def test_foreign_keys_and_transaction_rollback(topic):
    with pytest.raises(sqlite3.IntegrityError):
        with db.transaction() as connection:
            connection.execute("INSERT INTO messages(topic_id,role,body,created_at) VALUES(?,?,?,?)", (topic["id"] + 10000, "observation", "项目观察", db.now()))
    assert not db.query("SELECT * FROM messages")


def test_untrusted_markdown_is_inert():
    rendered = str(render_markdown("<script>alert(1)</script>\n\n[链接](javascript:alert(1))\n\n![远程图片](https://example.com/image.png)"))
    assert "<script>" not in rendered
    assert "href=\"javascript:" not in rendered
    assert "<img" not in rendered


def test_source_validation_and_malformed_model_result():
    assert source_url("https://docs.bigmodel.cn/cn/guide/tools/web-search#section") == "https://docs.bigmodel.cn/cn/guide/tools/web-search"
    from researcher.providers import ResearchError
    with pytest.raises(ResearchError):
        source_url("javascript:alert(1)")
    with pytest.raises(ValueError):
        structured_json("输出未完成")


def test_worker_restart_and_exclusive_execution(topic):
    from researcher.config import ROOT
    from researcher.app import worker_alive
    for attempt in range(2):
        worker = subprocess.Popen([sys.executable, "-m", "researcher.cli", "worker"], cwd=ROOT, env=os.environ.copy(), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            for _ in range(60):
                if worker_alive():
                    break
                assert worker.poll() is None
                time.sleep(0.1)
            else:
                pytest.fail("后台服务未产生心跳")
            duplicate = subprocess.run([sys.executable, "-m", "researcher.cli", "worker"], cwd=ROOT, env=os.environ.copy(), capture_output=True, timeout=10)
            assert duplicate.returncode != 0
            assert "已有 worker" in duplicate.stderr.decode()
            assert db.one("SELECT title FROM topics WHERE id=?", (topic["id"],))["title"] == topic["title"]
        finally:
            worker.terminate()
            worker.wait(timeout=15)
        assert not worker_alive()
        assert worker.returncode == 0
