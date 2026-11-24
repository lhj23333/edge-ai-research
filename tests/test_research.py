import asyncio
import secrets

import pytest

from researcher import continuity, db, research, station
from researcher.config import Profile, ResearchSettings, save_profile, settings, write_json
from researcher.knowledge import public_url
from researcher.providers import ResearchError
from researcher.research_engine import ResearchRun
from researcher.research_models import Gap, LEGACY_PROMPT_VERSION
from researcher.scheduler import tick
from researcher.worker import claim_task


def test_research_entry_and_csrf(client, csrf):
    response = client.get("/research")
    assert response.status_code == 200
    assert "提出研究目标" in response.text
    assert client.post("/research", data={"message": "研究已有公开资料中的部署条件"}).status_code == 403
    response = client.post("/research", data={"csrf": csrf, "message": "研究已有公开资料中的部署条件"})
    assert "请在设置中添加并选择模型配置" in response.text
    assert not db.query("SELECT * FROM research_sessions")


def test_research_budget_and_settings_persist(client, csrf):
    response = client.post("/settings/research", data={"csrf": csrf, "question_requests": 18, "automatic_daily_requests": 20, "device": "cpu"})
    assert response.status_code == 200
    assert settings()["research"]["question_requests"] == 18
    assert settings()["research"]["device"] == "cpu"
    client.post("/settings/limits", data={"csrf": csrf, "daily_requests": 60, "task_requests": 8, "output_tokens": 4096})
    assert settings()["research"]["question_requests"] == 18
    client.post("/settings/research", data={"csrf": csrf, "question_requests": 0})
    assert settings()["research"]["question_requests"] == 18


def test_empty_research_session_and_evidence_are_not_created(client, csrf):
    assert client.get("/research/999999").status_code == 404
    assert client.post("/research/999999/messages", data={"csrf": csrf, "message": "继续检查原文"}).status_code == 404
    assert db.one("SELECT count(*) AS n FROM claim_evidence")["n"] == 0
    assert db.one("SELECT count(*) AS n FROM knowledge_items")["n"] == 0


@pytest.mark.parametrize(("kind", "expected"), [("research", "interactive"), ("chat", "interactive"), ("summary", "revise")])
def test_historical_topic_tasks_keep_directed_scope(client, csrf, topic, kind, expected):
    profile = Profile(id="glm", name="智谱", provider="zhipu", model="GLM-5.2", base_url="https://open.bigmodel.cn/api/paas/v4",
                      chat_verified=db.now(), search_verified=db.now())
    save_profile(profile, secrets.token_urlsafe())
    path = f"/topics/{topic['id']}/tasks"
    assert client.post(path, data={"kind": kind}).status_code == 403
    fields = {"csrf": csrf, "kind": kind}
    if kind == "chat":
        fields["prompt"] = "核查 Hailo-10H 的测试条件"
    response = client.post(path, data=fields, follow_redirects=False)
    task = db.one("SELECT * FROM tasks")
    assert response.status_code == 303 and response.headers["location"] == f"/tasks/{task['id']}"
    assert task["kind"] == expected and task["topic_id"] == topic["id"] and task["session_id"]
    assert task["priority"] > 0 and not task["automatic"]
    run = ResearchRun(task)
    assert run.base_context()["target"]["title"] == topic["title"]
    assert [scope["id"] for scope in run.scope["topics"]] == [topic["id"]]
    assert bool(run.can_search) == (kind != "summary")
    if kind == "chat":
        assert run.goal == fields["prompt"]
    assert not db.query("SELECT id FROM feed_runs")


def test_historical_topic_chat_can_continue_with_its_original_brief(client, csrf, topic):
    profile = Profile(id="glm", name="智谱", provider="zhipu", model="GLM-5.2", base_url="https://open.bigmodel.cn/api/paas/v4",
                      chat_verified=db.now())
    save_profile(profile, secrets.token_urlsafe())
    original = db.execute("INSERT INTO tasks(topic_id,kind,status,profile_id,profile_fingerprint,provider,model,created_at) VALUES(?,'research','success','','','','',?)",
                          (topic["id"], db.now()))
    brief = db.execute("INSERT INTO briefs(topic_id,task_id,title,body,created_at) VALUES(?,?,?,?,?)",
                       (topic["id"], original, "Hailo-10H 上市历史简报", "Hailo 公告称已开始接受订单。", db.now()))
    fields = {"csrf": csrf, "kind": "chat", "prompt": "核对旧简报中的公开发售日期", "brief_id": brief}
    path = f"/topics/{topic['id']}/tasks"
    rejected = client.post(path, data={**fields, "brief_id": 9223372036854775808})
    assert "记录编号必须是" in rejected.text
    assert not db.one("SELECT id FROM research_sessions")
    assert client.post(path, data=fields, follow_redirects=False).status_code == 303
    task = db.one("SELECT * FROM tasks WHERE session_id IS NOT NULL")
    assert ResearchRun(task).base_context()["historical_brief"]["title"] == "Hailo-10H 上市历史简报"
    client.post(f"/tasks/{task['id']}/cancel", data={"csrf": csrf}, follow_redirects=False)
    continued = client.post(f"/research/{task['session_id']}/messages", data={"csrf": csrf, "message": "继续核对官方发售公告"},
                            follow_redirects=False)
    assert continued.headers["location"] == f"/research/{task['session_id']}"
    latest = db.one("SELECT * FROM tasks WHERE session_id=? ORDER BY id DESC LIMIT 1", (task["session_id"],))
    run = ResearchRun(latest)
    assert run.base_context()["target"]["id"] == topic["id"]
    assert latest["context_brief"] == brief


def test_tick_enqueues_only_authorized_open_due_gaps_and_respects_daily_quota(client, csrf):
    profile = Profile(id="glm", name="智谱", provider="zhipu", model="GLM-5.2", base_url="https://open.bigmodel.cn/api/paas/v4",
                      chat_verified=db.now())
    save_profile(profile, secrets.token_urlsafe())
    config = settings()
    config["research"]["followups_per_day"] = 1
    write_json("config.json", config)

    def store_gap(question, priority, allowed=True):
        gap = Gap(question=question, priority=priority, reason="公开资料尚无完整实测条件。",
                  completion="找到标明型号和功耗测量方式的一手资料。")
        identifier = research.save_gap(gap, {"id": None}, [])
        db.execute("UPDATE research_gaps SET automatic_allowed=? WHERE id=?", (int(allowed), identifier))
        return identifier

    authorized = store_gap("核验 Hailo-10H 的功耗测量条件", 3)
    excess = store_gap("核验 Hailo-8 的单批量延迟数据", 1)
    unauthorized = store_gap("核验 Edge TPU 支持的量化精度", 5, allowed=False)
    paused = store_gap("核验 Palette SDK 的部署前置条件", 5)
    future = store_gap("核验 HailoRT 新版本对主机的要求", 5)
    db.execute("UPDATE research_gaps SET next_check_at='2999-01-01T00:00:00+00:00' WHERE id=?", (future,))
    assert client.post(f"/research/gaps/{paused}/pause", data={"csrf": csrf}, follow_redirects=False).status_code == 303
    for gates in ("watchlist_confirmed=1,sources_confirmed=1", "auto_enabled=1,watchlist_confirmed=0", "watchlist_confirmed=1,sources_confirmed=0"):
        db.execute(f"UPDATE station_settings SET {gates} WHERE id=1")
        tick()
        assert not db.query("SELECT id FROM tasks")
    db.execute("UPDATE station_settings SET sources_confirmed=1 WHERE id=1")
    tick()
    tick()
    task = db.one("SELECT * FROM tasks")
    assert task["kind"] == "followup" and task["automatic"] == 1 and task["priority"] == 0
    assert station.decode(task["job_json"])["gap_id"] == authorized
    assert db.one("SELECT task_id,status,attempts FROM research_gaps WHERE id=?", (authorized,)) == {"task_id": task["id"], "status": "running", "attempts": 1}
    assert claim_task(True) is None
    assert client.post(f"/research/gaps/{authorized}/pause", data={"csrf": csrf}, follow_redirects=False).status_code == 303
    research.finish_gap(task, False)
    tick()
    assert db.query("SELECT status FROM tasks") == [{"status": "cancelled"}]
    gaps = {row["id"]: row for row in db.query("SELECT id,status,task_id FROM research_gaps")}
    assert gaps[paused]["status"] == gaps[authorized]["status"] == "paused"
    assert all(gaps[identifier]["task_id"] is None for identifier in (paused, excess, unauthorized, future))


@pytest.mark.parametrize("schema", [6, 7])
def test_upgrade_reuses_research_snapshot_with_old_ingest_settings(schema):
    profile = Profile(id="glm", name="智谱", provider="zhipu", model="GLM-5.2", base_url="https://open.bigmodel.cn/api/paas/v4", chat_verified=db.now())
    save_profile(profile, secrets.token_urlsafe())
    goal = "核验 Hailo-10H 的产品资料对生成式 AI 用途的说明"
    task_id = research.enqueue("interactive", {"main_question": goal})
    task = db.one("SELECT * FROM tasks WHERE id=?", (task_id,))
    job = station.decode(task["job_json"])
    if schema == 6:
        del job["routes_fingerprint"], job["prompt_version"]
        db.execute("UPDATE tasks SET job_json=? WHERE id=?", (station.encode(job), task_id))
        task["job_json"] = station.encode(job)
    run = ResearchRun(task)
    key = f"planner:{run.prompt_version}:0"

    async def build():
        return {"messages": [{"role": "user", "content": goal}], "tools": []}

    prepared = asyncio.run(run.prepare_request(key, build))
    current_config = settings()
    research_values = ResearchSettings(**current_config["research"]).model_dump()
    if schema == 6:
        del research_values["ingest_requests"], research_values["automatic_event_research"]
        research_values["scan_requests"] = 21
        assert run.prompt_version == LEGACY_PROMPT_VERSION
        current_config["model_routes"]["deep_plan"] = {"reasoning_effort": "disabled", "output_tokens": 1024}
    else:
        research_values.update(ingest_requests=47, automatic_event_research=True)
    saved = {"job_hash": continuity.digest(job), "config_hash": continuity.digest(research_values), "profile_fingerprint": task["profile_fingerprint"]}
    db.execute("UPDATE research_operations SET inputs_json=? WHERE task_id=? AND operation_key=?",
               (station.encode(saved), task_id, "request_inputs:" + key))

    current_config["research"]["ingest_requests"] = 60
    write_json("config.json", current_config)
    resumed = ResearchRun(task)
    assert resumed.saved_request(key) == prepared
    assert resumed.gateway.route.reasoning_effort == "high" and resumed.gateway.route.output_tokens == 8192

    current_config["research"]["max_rounds"] += 1
    write_json("config.json", current_config)
    with pytest.raises(ResearchError, match="研究配置已改变"):
        ResearchRun(task).saved_request(key)


@pytest.mark.parametrize("url", ["http://127.0.0.1", "http://169.254.169.254", "http://[::1]", "http://localhost"])
def test_source_reader_rejects_private_network(url):
    with pytest.raises(ResearchError):
        asyncio.run(public_url(url))
