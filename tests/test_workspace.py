import json
import sqlite3
import os
import subprocess
import sys
import time
import zipfile
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from researcher import db
from researcher.app import render_markdown
from researcher.cli import backup, lock_file, restore
from researcher.config import DATA, Profile, get_profile, settings
from researcher.providers import source_url
from researcher.scheduler import next_run
from researcher.service import context, enqueue, save_artifact
from researcher.worker import parse_research


@pytest.mark.parametrize("path", ["/", "/updates", "/map", "/map/data", "/companies", "/compare", "/evidence", "/questions", "/personal", "/watchlist", "/topics", "/library", "/settings", "/tasks", "/health", "/static/style.css", "/static/station.css", "/static/station.js", "/static/app.js", "/static/htmx.min.js"])
def test_routes_and_headers(client, path):
    response = client.get(path)
    assert response.status_code == 200
    assert response.headers["x-frame-options"] == "DENY"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]


def test_create_edit_observation_and_context(client, csrf, topic):
    path = f"/topics/{topic['id']}"
    assert client.get(path).status_code == 200
    assert client.post(path + "/edit", data={"csrf": csrf, "title": topic["title"], "question": "重点关注模型支持与部署条件。"}).status_code == 200
    observation = "Demo 希望通过阅读和讨论逐步积累研究成果。"
    assert client.post(path + "/observations", data={"csrf": csrf, "body": observation}).status_code == 200
    assert observation not in context(topic["id"])
    assert db.one("SELECT body FROM annotations WHERE target_kind='topic' AND target_id=?", (topic["id"],))["body"] == observation
    assert db.one("SELECT * FROM topics WHERE id=?", (topic["id"],))["question"] == "重点关注模型支持与部署条件。"


def test_csrf_and_origin_validation(client, topic):
    path = f"/topics/{topic['id']}/observations"
    assert client.post(path, data={"body": "研究观察"}).status_code == 403
    assert client.post(path, data={"csrf": client.cookies["researcher_csrf"], "body": "研究观察"}, headers={"origin": "https://example.com"}).status_code == 403
    assert client.get("/", headers={"host": "external.example"}).status_code == 400
    assert not db.query("SELECT * FROM messages")


def test_no_key_blocks_research_and_automation(client, csrf, topic):
    response = client.post("/updates/scan", data={"csrf": csrf})
    assert "请在设置中添加并选择模型配置" in response.text
    response = client.post("/watchlist/automation", data={"csrf": csrf, "enabled": "yes", "schedule_time": "09:00"})
    assert "请先确认关注名单" in response.text
    assert db.one("SELECT schedule FROM topics WHERE id=?", (topic["id"],))["schedule"] == "manual"
    assert not db.query("SELECT * FROM tasks")


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
    assert changed in context(topic["id"])


def test_limits_persist_and_reject_invalid_values(client, csrf):
    client.post("/settings/limits", data={"csrf": csrf, "daily_requests": 20, "task_requests": 5, "output_tokens": 2048})
    assert settings()["limits"]["daily_requests"] == 20
    client.post("/settings/limits", data={"csrf": csrf, "daily_requests": -1, "task_requests": 0, "output_tokens": 0})
    assert settings()["limits"]["daily_requests"] == 20
    assert (DATA / "config.json").stat().st_mode & 0o777 == 0o600
    assert DATA.stat().st_mode & 0o777 == 0o700


def test_profiles_reject_missing_keys_and_invalid_address(client, csrf):
    response = client.post("/settings/profiles", data={"csrf": csrf, "name": "通义千问", "provider": "qwen", "base_url": "https://dashscope.aliyuncs.com/api/v1", "model": "qwen-plus"})
    assert "首次保存配置时需要填写 API 密钥" in response.text
    assert not settings()["profiles"]
    with pytest.raises(ValidationError):
        Profile(id="qwen", name="通义千问", provider="qwen", base_url="http://127.0.0.1", model="qwen-plus")


def test_real_calendar_boundaries():
    friday = datetime(2026, 10, 30, 1, 0, tzinfo=timezone.utc)
    assert next_run("daily", "09:00", 0, "Asia/Shanghai", friday) == "2026-10-31T01:00:00+00:00"
    assert next_run("weekly", "09:00", 4, "Asia/Shanghai", friday) == "2026-11-06T01:00:00+00:00"
    assert next_run("weekly", "09:00", 0, "Asia/Shanghai", friday) == "2026-11-02T01:00:00+00:00"
    assert next_run("manual", "09:00", 0, "Asia/Shanghai", friday) is None


def test_backup_restore_and_running_service_guard(topic):
    first = save_artifact(topic["id"], "项目研究意图", topic["question"])
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
    with pytest.raises(ResearchError):
        parse_research("输出未完成")


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
