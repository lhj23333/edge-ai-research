import asyncio
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote, urlsplit

import httpx
import pytest
from lxml import html

from researcher import continuity, db, knowledge, research, station
from researcher.config import COST_PROFILES, Profile, ResearchSettings, save_profile, settings, write_json
from researcher.knowledge import public_url
from researcher.providers import BudgetExceeded, ResearchError, Source
from researcher.research_engine import ResearchRun, perform, route_review
from researcher.research_models import Draft, Finding, Gap, LEGACY_PROMPT_VERSION, Point
from researcher.research_review import partial_review
from researcher.scheduler import release_expired, schedule_directed_research, tick
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


def test_research_routes_accept_requests_with_a_tight_safety_limit(client, csrf):
    profile = Profile(id="glm", name="智谱", provider="zhipu", model="GLM-5.2", base_url="https://open.bigmodel.cn/api/paas/v4",
                      chat_verified=db.now(), input_price=8, output_price=28)
    save_profile(profile, secrets.token_urlsafe())
    payload = {"csrf": csrf, "message": "核对 Hailo-10H 的公开部署条件"}
    allowed = client.post("/research", data=payload, follow_redirects=False)
    session_path = allowed.headers["location"]
    task = db.one("SELECT id FROM tasks")["id"]
    client.post(f"/tasks/{task}/cancel", data={"csrf": csrf}, follow_redirects=False)
    source = Source("Hailo-10H", "https://hailo.ai/products/ai-accelerators/hailo-10h-ai-accelerator/",
                    "Hailo-10H supports edge generative AI applications.", "公开资料")
    document, version = station.save_document(source, None)
    claim = station.save_claim(document, version, source.content, source.content, "厂商宣称")
    event, _ = station.add_event({"identity_key": "hailo-10h-availability", "event_type": "产品发布", "title": source.title,
                                  "what_happened": source.content, "why_important": "支持边缘生成式推理。"}, [claim], [], [])
    config = settings()
    config["limits"]["daily_cost"] = 0.000001
    write_json("config.json", config)
    for path in ("/research", session_path + "/messages", f"/events/{event}/research"):
        result = client.post(path, data={**payload, "force_refresh": "yes"}, follow_redirects=False)
        assert result.status_code == 303 and not urlsplit(result.headers["location"]).query
        assert result.headers["location"].startswith("/research/")
    assert db.one("SELECT count(*) AS n FROM tasks WHERE status='queued'")["n"] == 3


def cost_profile_for_research(name="balanced", limit=None):
    profile = Profile(id="glm", name="智谱", provider="zhipu", model="GLM-5.2", base_url="https://open.bigmodel.cn/api/paas/v4",
                      chat_verified=db.now(), input_price=8, output_price=28)
    save_profile(profile, secrets.token_urlsafe())
    config = settings()
    config.update(cost_profile=name)
    config["limits"]["daily_cost"] = limit
    write_json("config.json", config)


def archived_hailo():
    source = Source("Hailo-10H", "https://hailo.ai/products/ai-accelerators/hailo-10h-ai-accelerator/",
                    "Hailo-10H supports edge generative AI applications in a standard M.2 form factor.", "公开资料")
    document, version = station.save_document(source, None)
    with db.transaction() as connection:
        station.save_document_chunk(connection, version, 0, 0, None, source.title, source.content)
    chunk = db.one("SELECT id FROM document_chunks WHERE version_id=?", (version,))["id"]
    return source, document, version, {**knowledge.chunk_row(chunk), "chunk_id": chunk}


def test_redline_keeps_latest_draft_and_unverified_claims_stay_out_of_knowledge():
    cost_profile_for_research(limit=0.000001)
    source, _, _, chunk = archived_hailo()
    task_id = research.enqueue("interactive", {"main_question": "核查 Hailo-10H 的部署形态"})
    task = claim_task(True)
    assert task["id"] == task_id
    run = ResearchRun(task)
    draft = Draft(findings=[Finding(ref="hailo", statement=source.content, label="厂商宣称", chunk_ids=[chunk["chunk_id"]])],
                  points=[Point(category="来源陈述", text=source.content, finding_refs=["hailo"])],
                  complete=False, completion_notes="已有产品说明，仍需核验部署条件。")
    state = {"draft": draft.model_dump(), "chunks": [chunk], "errors": [], "finish": {}, "round": 1}
    state.update(asyncio.run(run.review(state)))
    assert route_review(state) == "commit"
    result = asyncio.run(run.commit(state))["outcome"]
    assert result["degraded_reason"] == "cost_safety_limit"
    assert result["draft"] == draft.model_dump() and source.content in result["answer"]
    assert db.one("SELECT status FROM tasks WHERE id=?", (task_id,))["status"] == "success"
    assert not db.query("SELECT id FROM knowledge_items")
    assert not db.query("SELECT id FROM model_call_ledger")


def test_redline_before_planning_finishes_the_graph_without_model_calls():
    cost_profile_for_research(limit=0.000001)
    task_id = research.enqueue("interactive", {"main_question": "核查 Hailo-10H 的部署形态"})
    asyncio.run(perform(claim_task(True)))
    task = db.one("SELECT status,result_json FROM tasks WHERE id=?", (task_id,))
    assert task["status"] == "success"
    assert station.decode(task["result_json"])["degraded_reason"] == "cost_safety_limit"
    assert not db.query("SELECT id FROM model_call_ledger")


@pytest.mark.parametrize("name", ["eco", "balanced", "deep"])
def test_review_round_limit_commits_the_current_draft(name):
    cost_profile_for_research(name)
    task_id = research.enqueue("interactive", {"main_question": "核查 Hailo-10H 的部署形态"})
    run = ResearchRun(claim_task(True))
    assert run.task["id"] == task_id
    source, _, _, chunk = archived_hailo()
    draft = Draft(findings=[Finding(ref="hailo", statement=source.content, label="厂商宣称", chunk_ids=[chunk["chunk_id"]])],
                  complete=False, completion_notes="已有产品说明，仍需核验部署条件。")
    state = {"draft": draft.model_dump(), "review_round": COST_PROFILES[name].review_rounds - 1,
             "chunks": [chunk], "finish": {}, "errors": [], "round": 1}
    reviewed = partial_review(draft, "需要更多公开部署资料").model_dump()
    state.update(run.review_outcome(state, {"review": reviewed}))
    assert route_review(state) == "commit"
    result = asyncio.run(run.commit(state))["outcome"]
    assert result["degraded_reason"] == "review_round_limit"
    assert result["draft"] == draft.model_dump()
    assert result["review_rounds"] == COST_PROFILES[name].review_rounds


@pytest.mark.parametrize("target_kind", ["topic", "event"])
def test_unchanged_evidence_reuses_research_and_new_source_version_invalidates_it(topic, target_kind):
    cost_profile_for_research()
    source, document, version, chunk = archived_hailo()
    claim = station.save_claim(document, version, source.content, source.content, "厂商宣称")
    event, _ = station.add_event({"identity_key": "hailo-10h-availability", "event_type": "产品发布", "title": source.title,
                                  "what_happened": source.content, "why_important": "支持边缘生成式推理。"}, [claim], [], [topic["id"]])

    def enqueue():
        if target_kind == "event":
            return research.enqueue_event(event)
        return research.enqueue("revise", research.topic_job(topic))

    first = enqueue()
    run = ResearchRun(claim_task(True))
    assert run.unchanged_evidence() is None
    hash_before = run.evidence_hash
    draft = Draft(complete=False, completion_notes="当前只保存已归档的资料")
    state = {"draft": draft.model_dump(), "review": partial_review(draft, "当前只保存已归档的资料").model_dump(),
             "chunks": [chunk], "finish": {}, "errors": [], "round": 0}
    assert asyncio.run(run.commit(state))["outcome"]["evidence_hash"] == hash_before

    automatic = target_kind == "event"
    second = research.enqueue_event(event, automatic=True) if automatic else enqueue()
    asyncio.run(perform(claim_task(not automatic)))
    result = station.decode(db.one("SELECT result_json FROM tasks WHERE id=?", (second,))["result_json"])
    assert result["evidence_hash"] == hash_before
    assert result["reused_task_id"] == first and result["skipped_reason"] == "evidence_unchanged"
    assert not db.query("SELECT id FROM model_call_ledger")
    if automatic:
        assert research.enqueue_event(event, automatic=True) == second

    updated = Source(source.title, source.url, source.content + "\nHailo-10H supports vision language models.", source.extent)
    assert station.save_document(updated, None)[1] != version
    third = research.enqueue_event(event, automatic=True) if automatic else enqueue()
    current = ResearchRun(claim_task(not automatic))
    assert current.task["id"] == third and current.unchanged_evidence() is None
    assert current.evidence_hash != hash_before


def test_topic_schedule_quota_keeps_attempts_after_timeout(topic):
    cost_profile_for_research("eco")
    now = datetime.now(timezone.utc)
    checked = now.isoformat(timespec="seconds")
    due = (now - timedelta(minutes=1)).isoformat(timespec="seconds")
    db.execute("UPDATE station_settings SET auto_enabled=1,watchlist_confirmed=1,sources_confirmed=1 WHERE id=1")
    db.execute("INSERT INTO topic_research_schedules(topic_id,frequency,schedule_time,weekday,next_run) VALUES(?,'daily','09:00',0,?)",
               (topic["id"], due))
    schedule_directed_research()
    task = claim_task(False)
    gateway = ResearchRun(task).gateway
    payload = {"messages": [{"role": "user", "content": topic["question"]}], "max_tokens": 128}
    for _ in range(COST_PROFILES["eco"].daily_topic_calls):
        gateway.reserve("plan", gateway.request_cost(payload, "plan"), tokens=gateway.request_tokens(payload, "plan"))
    with pytest.raises(BudgetExceeded, match="定时专题研究"):
        gateway.reserve("plan", gateway.request_cost(payload, "plan"))
    db.execute("UPDATE tasks SET status='queued' WHERE id=?", (task["id"],))
    db.execute("UPDATE topic_retry_quotas SET auto_released_at=? WHERE task_id=?", (due, task["id"]))
    release_expired(checked)
    quota = db.one("SELECT attempt_count,status FROM topic_retry_quotas WHERE task_id=?", (task["id"],))
    assert quota == {"attempt_count": 8, "status": "expired"}
    assert db.one("SELECT status FROM tasks WHERE id=?", (task["id"],))["status"] == "interrupted"
    assert db.one("SELECT max(attempt_count) AS attempts FROM model_call_ledger WHERE task_id=?", (task["id"],))["attempts"] == 8


def test_session_feedback_remains_visible_with_archived_citations(client, csrf, served_folder):
    profile = Profile(id="glm", name="智谱", provider="zhipu", model="GLM-5.2", base_url="https://open.bigmodel.cn/api/paas/v4",
                      chat_verified=db.now())
    save_profile(profile, secrets.token_urlsafe())
    task_id = research.enqueue("interactive", {"main_question": "核验 Hailo-10H 的部署形态"})
    task = db.one("SELECT session_id FROM tasks WHERE id=?", (task_id,))
    path = f"/research/{task['session_id']}"
    statement = "Hailo-10H is available in a standard M.2 form factor."
    url = served_folder + "/hailo-10h.txt"
    source = Source("Hailo-10H", url, statement, "用户提供摘录")
    document, version = station.save_document(source, task_id)
    claim = station.save_claim(document, version, statement, statement, "厂商宣称", manual_excerpt=True)
    chunk = db.one("SELECT id FROM document_chunks WHERE version_id=?", (version,))["id"]
    item = knowledge.put_knowledge(statement, "事实", [claim], task_id)
    db.execute("UPDATE tasks SET status='success',result_json=? WHERE id=?",
               (station.encode({"knowledge_ids": [item], "document_ids": [document], "chunk_ids": [chunk]}), task_id))
    db.execute("INSERT INTO research_messages(session_id,task_id,role,body,created_at) VALUES(?,?,'assistant',?,?)",
               (task["session_id"], task_id, statement, db.now()))
    with httpx.Client(trust_env=False) as requester:
        response = requester.get(url)
        with pytest.raises(httpx.HTTPStatusError) as failure:
            response.raise_for_status()
    knowledge.record_fetch_failure(document, failure.value)
    feedback = client.post(path + "/feedback", data={"csrf": csrf, "task_id": task_id, "item_id": item,
                                                     "kind": "outdated", "body": "请复核模块形态的适用版本"}, follow_redirects=False)
    assert feedback.status_code == 303 and "反馈已保存" in unquote(feedback.headers["location"])
    page = client.get(path)
    tree = html.fromstring(page.content)
    assert all(part in page.text for part in ("历史反馈记录", "已经过时", "请复核模块形态的适用版本", "当前核验状态：需要重新核验", statement, "HTTP 404"))
    assert tree.xpath(f"//a[normalize-space(.)='本地归档原文']/@href") == [f"/documents/{document}?version={version}#chunk-{chunk}"]
    assert tree.xpath("//a[contains(.,'外部原文链接')]/@href") == [url]
    assert client.get(f"/documents/{document}?version={version}").status_code == 200
    assert "请优先查看本地归档" in client.get("/evidence").text


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
    assert (resumed.gateway.route.reasoning_effort, resumed.gateway.route.output_tokens) == (("high", 8192) if schema == 6 else ("low", 4096))

    current_config["research"]["max_rounds"] += 1
    write_json("config.json", current_config)
    with pytest.raises(ResearchError, match="研究配置已改变"):
        ResearchRun(task).saved_request(key)


@pytest.mark.parametrize("url", ["http://127.0.0.1", "http://169.254.169.254", "http://[::1]", "http://localhost"])
def test_source_reader_rejects_private_network(url):
    with pytest.raises(ResearchError):
        asyncio.run(public_url(url))
