import asyncio
import json
import secrets
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
from lxml import html

from researcher import db, knowledge, station
from researcher.config import Profile, save_profile, settings, write_json
from researcher.industry import selection
from researcher.ingest import benchmark, jobs
from researcher.ingest.cluster import Decision, commit
from researcher.ingest.collectors import FeedConfig, save_source, stamp
from researcher.ingest.funnel import NamedEntity, Writeup, save_original
from researcher.ingest.gate import processing_source
from researcher.providers import ResearchError, Source
from researcher.research_engine import ResearchRun
from researcher.station_web import reading_snapshot
from researcher.worker import claim_task


@pytest.mark.parametrize("path", ["/admin/sources", "/admin/funnel", "/admin/eval", "/settings", "/updates?view=all", "/updates?view=heat"])
def test_pages_render(client, path):
    assert client.get(path).status_code == 200


def test_preview_and_collect_endpoint_use_the_same_publication_rules(client, csrf, raspberry_feed):
    source = FeedConfig(key="raspberry-pi", name="Raspberry Pi", kind="rss", tier="T1.5", url=raspberry_feed, publisher="raspberry-pi", enabled=True)
    data = {"csrf": csrf, "source": source.model_dump_json()}
    preview = client.post("/admin/sources/preview", data=data, headers={"Accept": "application/json"})
    assert preview.status_code == 200
    assert preview.json()["items"][0]["title"] == "Citizen science with Raspberry Pi"
    assert preview.json()["items"][0]["published_at"] == "2026-11-23T10:26:34+00:00"
    assert not db.query("SELECT id FROM feed_items") and not db.query("SELECT id FROM feed_runs")
    assert client.post("/admin/sources", data=data, follow_redirects=False).status_code == 303
    queued = client.post("/updates/collect", data={"csrf": csrf}, follow_redirects=False)
    assert queued.status_code == 303
    task = db.one("SELECT * FROM tasks")
    assert task["kind"] == "feed" and not task["session_id"] and task["priority"] < 0
    assert station.decode(task["job_json"])["source_ids"] == [db.one("SELECT id FROM feed_sources WHERE source_key='raspberry-pi'")["id"]]
    assert client.get(queued.headers["location"]).status_code == 200


def test_routes_and_event_research_bind_local_evidence(client, csrf):
    profile = Profile(id="deepseek", name="DeepSeek", provider="deepseek", model="deepseek-chat", base_url="https://api.deepseek.com/v1", chat_verified=db.now())
    save_profile(profile, secrets.token_urlsafe())
    source = Source("Hailo-10H AI Accelerator", "https://hailo.ai/products/ai-accelerators/hailo-10h-ai-accelerator/", "Hailo-10H supports edge generative AI applications.", "公开资料", "2025-07-23T00:00:00+00:00")
    document, version = station.save_document(source, None)
    claim = station.save_claim(document, version, source.content, source.content, "厂商宣称")
    event, _ = station.add_event({"identity_key": "hailo|hailo-10h|edge-generative-ai", "event_type": "产品发布", "title": source.title, "what_happened": source.content, "why_important": "边缘生成式推理能力。"}, [claim], [], [])
    assert knowledge.event_documents(event)[0]["version_id"] == version
    assert "多源时间线" in client.get(f"/events/{event}?timeline=desc").text
    response = client.post("/settings/routes", data={"csrf": csrf, "prefilter_profile_id": profile.id, "prefilter_output_tokens": 384})
    assert response.status_code == 200
    assert settings()["model_routes"]["prefilter"]["output_tokens"] == 384
    for _ in range(2):
        queued = client.post(f"/events/{event}/research", data={"csrf": csrf}, follow_redirects=False)
        assert queued.status_code == 303
    task, = db.query("SELECT * FROM tasks")
    assert json.loads(task["job_json"])["target_id"] == event
    assert queued.headers["location"] == f"/research/{task['session_id']}"
    assert not db.query("SELECT id FROM usage")


def commit_feed_report(source_id, source, writeup, now, score, category=""):
    document, version = station.save_document(Source(writeup.title, source["url"], writeup.summary, "公开资料", stamp(now)), None)
    item_id = db.execute("INSERT INTO feed_items(source_id,canonical_url,original_url,title,category,published_at,discovered_at,tier,publisher_key,document_id,version_id,score,state,updated_at) "
                         "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'selected',?)",
                         (source_id, source["url"], source["url"], writeup.title, category, stamp(now), stamp(now), source["tier"], source["publisher"], document, version, score, stamp(now)))
    item = db.one("SELECT * FROM feed_items WHERE id=?", (item_id,))
    event = commit(item, writeup, Decision(relation="new_event", confidence="high", event_id=None, reason="首次归档发布资料"), [], False, now, selection())
    return item, event


def test_event_page_queues_topic_revision_using_local_evidence(client, csrf):
    topic = {"kind": "专题", "name": "Hailo 边缘推理", "question": "Hailo 的板卡和 SDK 有哪些部署条件？"}
    assert client.post("/watchlist/add", data={"csrf": csrf, **topic}, follow_redirects=False).status_code == 303
    topic_id = db.one("SELECT id FROM topics WHERE title=?", (topic["name"],))["id"]
    profile = Profile(id="deepseek", name="DeepSeek", provider="deepseek", model="deepseek-chat", base_url="https://api.deepseek.com/v1",
                      chat_verified=db.now(), search_verified=db.now())
    save_profile(profile, secrets.token_urlsafe())
    source = {"url": "https://hailo.ai/products/ai-accelerators/hailo-10h-ai-accelerator/", "tier": "T1", "publisher": "hailo"}
    publisher = save_source(FeedConfig(key="hailo", name="Hailo", kind="external", tier=source["tier"], publisher=source["publisher"], enabled=True))
    writeup = Writeup(title="Hailo-10H AI Accelerator", summary="Hailo-10H supports edge generative AI applications.",
                      importance_reason="边缘生成式推理设备", event_type="产品发布", topic_ids=[topic_id])
    item, event = commit_feed_report(publisher, source, writeup, datetime.now(timezone.utc), 96)
    with db.transaction() as connection:
        station.save_document_chunk(connection, item["version_id"], 0, 0, None, writeup.title, writeup.summary)
    response = client.get(f"/events/{event}")
    assert response.status_code == 200 and "刷新相关专题判断" in response.text and "无需联网搜索" in response.text
    action = f"/knowledge/{topic_id}/revise"
    form, = html.fromstring(response.content).xpath(f"//form[@method='post' and @action='{action}']")
    form_data = dict(form.form_values())
    assert form_data == {"csrf": csrf, "event_id": str(event)}
    assert client.post(action, data={"event_id": event}).status_code == 403
    assert "error=" in client.post(action, data={"csrf": csrf, "event_id": event + 1}, follow_redirects=False).headers["location"]
    queued = client.post(action, data=form_data, follow_redirects=False)
    assert queued.status_code == 303
    task, = db.query("SELECT * FROM tasks")
    assert (task["kind"], task["status"], task["topic_id"], task["automatic"]) == ("revise", "queued", topic_id, 0)
    assert queued.headers["location"] == f"/tasks/{task['id']}"
    run = ResearchRun(claim_task(foreground=True))
    context = run.base_context()
    assert context["target"]["id"] == topic_id and context["trigger_event"]["id"] == event
    assert context["revision_purposes"] == ["行业竞争", "部署决策"]
    assert {entry["id"] for entry in context["scope"]["topics"]} == {topic_id}
    assert not run.can_search and not run.can_read and not context["refresh_required"]
    for name, arguments, error in (("search_web", {"query": writeup.title}, "未启用联网搜索"), ("read_source", {"url": source["url"]}, "限定为已归档资料")):
        tool_call = {"id": name, "function": {"name": name, "arguments": json.dumps(arguments)}}
        with pytest.raises(ResearchError, match=error):
            asyncio.run(run.execute_tool(tool_call, {"visited": []}))
    chunk, = knowledge.event_chunks(event, 4)
    tool_call = {"id": "read_evidence", "function": {"name": "read_evidence", "arguments": json.dumps({"chunk_ids": [chunk["chunk_id"]]})}}
    result = asyncio.run(run.execute_tool(tool_call, {"visited": []}))
    assert result["chunks"][0]["content"] == writeup.summary and not db.query("SELECT id FROM usage")


def test_event_page_requires_multiple_distinct_high_score_events_since_topic_revision(client, csrf):
    assert client.post("/watchlist/add", data={"csrf": csrf, "kind": "专题", "name": "Hailo 边缘推理", "question": "软件发布影响哪些应用？"}, follow_redirects=False).status_code == 303
    topic_id = db.one("SELECT id FROM topics WHERE title='Hailo 边缘推理'")["id"]
    now = datetime.now(timezone.utc)
    source = {"url": "https://ai.zhiding.cn/2025/0723/3169328.shtml", "tier": "T2", "publisher": "zhiding"}
    publisher = save_source(FeedConfig(key="zhiding", name="至顶网", kind="external", tier="T2", publisher="zhiding", enabled=True))
    report = Writeup(title="Hailo-10H 边缘生成式 AI 加速器", summary="Hailo-10H 支持边缘生成式 AI 推理。", importance_reason="边缘生成式推理设备",
                     event_type="产品发布", entities=[NamedEntity(kind="厂商", name="Hailo")], topic_ids=[topic_id])
    item, event = commit_feed_report(publisher, source, report, now, 94)
    page = f"/events/{event}"
    assert "刷新相关专题判断" not in client.get(page).text
    next_report = report.model_copy(update={"topic_ids": [], "title": "Hailo-10H 进入商业部署", "event_type": "商业进展"})
    other = {**source, "url": "https://www.geekpark.net/news/351800"}
    _, other_event = commit_feed_report(publisher, other, next_report, now, 93)
    assert event != other_event and "刷新相关专题判断" in client.get(page).text
    claim = station.save_claim(item["document_id"], item["version_id"], report.summary, report.summary, "公开披露")
    station.revise(topic_id, "行业竞争", "已有边缘推理产品发布。", "初步判断", "公开资料", "待查商用规模", "整理已有资料", event, [claim], [])
    assert "刷新相关专题判断" not in client.get(page).text


def test_manual_score_over_threshold_requires_retry_to_restore_public_event(client, csrf):
    source = {"url": "https://hailo.ai/products/ai-accelerators/hailo-10h-ai-accelerator/", "tier": "T1", "publisher": "hailo"}
    publisher = save_source(FeedConfig(key="hailo", name="Hailo", kind="external", tier="T1", publisher="hailo", enabled=True))
    report = Writeup(title="Hailo-10H AI Accelerator", summary="Hailo-10H supports edge generative AI applications.", importance_reason="公开边缘推理规格", event_type="产品发布")
    item, event = commit_feed_report(publisher, source, report, datetime.now(timezone.utc), 88, category="开发者更新")
    action = f"/admin/funnel/{item['id']}"
    public_path = f"/events/{event}"
    assert public_path in html.fromstring(client.get("/updates").content).xpath("//a/@href")
    policy = selection()
    minimum = policy.threshold(item["tier"], item["category"])
    for score in (10, minimum - 1, minimum):
        response = client.post(f"{action}/score", data={"csrf": csrf, "score": score, "reason": "重新核对归档资料"}, follow_redirects=False)
        assert response.status_code == 303
        assert db.one("SELECT state,manual_score,event_id FROM feed_items WHERE id=?", (item["id"],)) == {"state": "scored_out", "manual_score": score, "event_id": event}
        assert public_path not in html.fromstring(client.get("/updates").content).xpath("//a/@href")
        detail = client.get(response.headers["location"])
        forms = html.fromstring(detail.content).xpath(f"//form[@action='{action}/retry']")
        assert bool(forms) == (score == minimum)
    assert "人工评分已保存，当前仍未公开展示为入选事件，请点击下方「继续处理」" in detail.text
    direct = client.get(f"/admin/funnel?item_id={item['id']}")
    assert "评分已达门槛，待继续处理" in direct.text
    assert "当前仍未公开展示为入选事件" in direct.text
    assert forms[0].xpath("./button[@class='button primary']/text()") == ["继续处理"]
    retry = client.post(forms[0].action, data=dict(forms[0].form_values()), follow_redirects=False)
    assert retry.status_code == 303 and retry.headers["location"].startswith("/tasks/")
    task = claim_task(foreground=False)
    assert retry.headers["location"] == f"/tasks/{task['id']}"
    asyncio.run(jobs.perform(task))
    assert db.one("SELECT status FROM tasks WHERE id=?", (task["id"],))["status"] == "success"
    assert db.one("SELECT state,event_id FROM feed_items WHERE id=?", (item["id"],)) == {"state": "clustered", "event_id": event}
    assert public_path in html.fromstring(client.get("/updates").content).xpath("//a/@href")


def test_legacy_event_context_renders_without_collection_metrics(client):
    source = Source("Hailo-10H AI Accelerator", "https://hailo.ai/products/ai-accelerators/hailo-10h-ai-accelerator/", "Hailo-10H supports edge generative AI applications.")
    document, version = station.save_document(source, None)
    claim = station.save_claim(document, version, source.content, source.content, "厂商宣称")
    event, _ = station.add_event({"identity_key": "hailo|10h|announce", "event_type": "产品发布", "title": source.title,
                                 "what_happened": source.content, "why_important": "边缘推理设备"}, [claim], [], [])
    legacy = db.one("SELECT id,title,event_type,what_happened,why_important,occurred_at,published_at,created_at,background,1 AS evidence_count FROM events WHERE id=?", (event,))
    for path in ("/", "/updates?view=all", f"/events/{event}"):
        response = client.get(path)
        assert response.status_code == 200
        assert source.title in response.template.render({**response.context, "events": [legacy], "event": legacy})
    updates = client.get("/updates?view=all")
    assert "0 分" in updates.template.render({**updates.context, "events": [dict(legacy, feed_score=0)]})


def test_redirect_duplicate_links_to_current_original_version(client, tmp_path, served_folder):
    published = db.now()
    original_url = served_folder + "/original.html"
    (tmp_path / "original.html").write_text(
        f'<html><head><meta property="article:published_time" content="{published}"></head><body>第二版原文</body></html>'
    )

    class RedirectHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(302)
            self.send_header("Location", original_url)
            self.end_headers()

    with ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler) as server:
        redirect_url = f"http://127.0.0.1:{server.server_port}/redirect"
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            source_id = save_source(FeedConfig(key="redirect-feed", name="重定向信源", kind="rss", tier="T2",
                                               url=served_folder + "/feed.xml", publisher="redirect-feed", enabled=True))
            document_id, prior_version = station.save_document(Source("同一资料", original_url, "第一版原文", "原文", published), None)
            _, current_version = station.save_document(Source("同一资料", original_url, "第二版原文", "原文", published), None)
            assert current_version > prior_version
            item_sql = ("INSERT INTO feed_items(source_id,canonical_url,original_url,title,published_at,discovered_at,tier,publisher_key,document_id,version_id,updated_at) "
                        "VALUES(?,?,?,'同一资料',?,?,'T2','redirect-feed',?,?,?)")
            db.execute(item_sql, (source_id, original_url, original_url, published, published, document_id, prior_version, published))
            duplicate_id = db.execute(item_sql, (source_id, redirect_url, redirect_url, published, published, None, None, published))
            item = db.one("SELECT * FROM feed_items WHERE id=?", (duplicate_id,))
            source = db.one("SELECT * FROM feed_sources WHERE id=?", (source_id,))
            task_id = jobs.enqueue()

            async def fetch_duplicate():
                async with httpx.AsyncClient(trust_env=False) as http:
                    return await save_original(item, source, task_id, http, None, selection())

            assert asyncio.run(fetch_duplicate()) is None
        finally:
            server.shutdown()
            thread.join()

    assert db.one("SELECT state,document_id,version_id FROM feed_items WHERE id=?", (duplicate_id,)) == {
        "state": "duplicate", "document_id": document_id, "version_id": current_version,
    }
    for version_id in (current_version, None):
        db.execute("UPDATE feed_items SET version_id=? WHERE id=?", (version_id, duplicate_id))
        response = client.get("/admin/funnel", params={"item_id": duplicate_id})
        assert response.status_code == 200
        href, = html.fromstring(response.content).xpath("//a[normalize-space(.)='保存的原文']/@href")
        expected = f"/documents/{document_id}"
        if version_id is not None:
            expected += f"?version={version_id}"
        assert href == expected
        original = client.get(href)
        assert original.status_code == 200
        assert original.context["selected_version"] == current_version


def test_selectbench_keeps_labeled_groups_separate():
    entries = benchmark.samples()
    development = [row for row in entries if row["split"] == "development"]
    holdout = [row for row in entries if row["split"] == "holdout"]
    assert len(development) == len(holdout) == 10
    assert {row["group"] for row in development}.isdisjoint(row["group"] for row in holdout)
    for split in (development, holdout):
        assert {row["tier"] for row in split} == {"T1", "T1.5", "T2"}
        assert sum(row["expected"] for row in split) == 5
        assert any(row["published_at"] is None and not row["expected"] for row in split)
        assert any(len({row["tier"] for row in split if row["group"] == item["group"]}) > 1 for item in split)
        report = benchmark.summarize(split, [], [])
        assert report["metrics"]["coverage"] == 0
        assert sum(metrics["total"] for metrics in report["by_tier"].values()) == len(split)


def test_selectbench_aggregates_tier_metrics_for_publication_date_baseline():
    entries = [row for row in benchmark.samples() if row["split"] == "development"]
    outcomes = [dict(row, selected=row["published_at"] is not None) for row in entries]
    report = benchmark.summarize(entries, outcomes, [])
    assert report["metrics"]["coverage"] == 1
    assert report["metrics"]["precision"] == pytest.approx(5 / 8)
    assert report["metrics"]["recall"] == 1
    assert report["metrics"]["f1"] == pytest.approx(10 / 13)
    assert report["by_tier"]["T1"]["f1"] == 1
    assert report["by_tier"]["T1.5"]["precision"] == pytest.approx(1 / 2)
    assert report["by_tier"]["T2"]["f1"] == pytest.approx(1 / 2)


def test_eval_queues_each_split_and_budget_stop_is_not_a_completed_report(client, csrf):
    profile = Profile(id="glm", name="智谱", provider="zhipu", model="glm-5.2", base_url="https://open.bigmodel.cn/api/paas/v4", input_price=10, output_price=10)
    save_profile(profile, secrets.token_urlsafe())
    config = settings()
    config["limits"]["daily_cost"] = 0.001
    write_json("config.json", config)

    def enqueue(split):
        response = client.post("/admin/eval", data={"csrf": csrf, "split": split}, follow_redirects=False)
        assert response.status_code == 303
        return response.headers["location"]

    development = enqueue("development")
    running = claim_task(False)
    assert development == f"/tasks/{running['id']}"
    holdout = enqueue("holdout")
    assert development != holdout
    assert enqueue("development") == development
    assert enqueue("holdout") == holdout
    for task in db.query("SELECT job_json FROM tasks ORDER BY id"):
        job = station.decode(task["job_json"])
        assert job["samples"] and all(row["split"] == job["split"] for row in job["samples"])
    assert running["request_limit"] == 3 * len(station.decode(running["job_json"])["samples"])
    asyncio.run(benchmark.perform(running))
    outcome = db.one("SELECT status,progress,error,result_json FROM tasks WHERE id=?", (running["id"],))
    assert outcome["status"] == "failed" and outcome["progress"] == "评测未完成"
    assert "每日" in outcome["error"] and "费用" in outcome["error"]
    report = station.decode(outcome["result_json"])
    assert report["metrics"]["coverage"] == 0
    assert sum(tier["total"] for tier in report["by_tier"].values()) == 10
    assert "查看评测任务与用量" not in client.get("/admin/eval").text
    assert not db.query("SELECT id FROM usage")


def test_home_marks_a_report_read_only_after_it_is_visible(client):
    published = datetime.fromisoformat("2025-07-23T04:00:00+00:00")
    policy = selection()
    items = []
    for key, title, url in (
        ("hailo", "Hailo-10H AI Accelerator", "https://hailo.ai/products/ai-accelerators/hailo-10h-ai-accelerator/"),
        ("zhiding", "Hailo-10H 边缘生成式 AI 加速器", "https://ai.zhiding.cn/2025/0723/3169328.shtml"),
    ):
        source = save_source(FeedConfig(key=key, name=key, kind="external", tier="T2", publisher=key, enabled=True))
        document, version = station.save_document(Source(title, url, "Hailo-10H 支持边缘生成式 AI 推理。", "公开资料", stamp(published)), None)
        identifier = db.execute("INSERT INTO feed_items(source_id,canonical_url,original_url,title,published_at,discovered_at,tier,publisher_key,document_id,version_id,score,state,updated_at) "
                                "VALUES(?,?,?,?,?,?,?,?,?,?,88,?,?)", (source, url, url, title, stamp(published), stamp(published), "T2", key, document, version, "selected", stamp(published)))
        items.append(db.one("SELECT * FROM feed_items WHERE id=?", (identifier,)))
    summary = Writeup(title=items[0]["title"], summary="Hailo-10H 支持边缘生成式 AI 推理。", importance_reason="公布边缘推理能力", event_type="产品发布")
    event = commit(items[0], summary, Decision(relation="new_event", confidence="high", event_id=None, reason="记录产品发布"), [], False, published, policy)

    def current_form():
        return {element.name: element.value for element in html.fromstring(client.get("/").text).xpath("//form[@id='reading-visit']//input")}

    old_form = current_form()
    replacement = Decision(relation="same_event", confidence="high", event_id=event, reason="媒体报道同一款产品")
    commit(items[1], summary, replacement, [], False, published, policy)
    newer_form = current_form()
    assert items[1]["discovered_at"] < old_form["reading_at"] < newer_form["reading_feed_at"]
    assert client.post("/reading/visit", data=old_form).status_code == 200
    state, latest, events = reading_snapshot(published + timedelta(hours=1))
    assert state["last_feed_visible_at"] == old_form["reading_feed_at"]
    assert events == [{"id": event, "title": summary.title, "visible_at": latest}]
    assert client.post("/reading/visit", data=newer_form).status_code == 200
    assert client.post("/reading/visit", data=old_form).status_code == 200
    assert reading_snapshot(published + timedelta(hours=1))[2] == []


def test_external_push_validates_session_and_deduplicates(client, csrf):
    source = FeedConfig(key="external-local", name="本机推送", kind="external", tier="T2", publisher="external-local", enabled=True)
    client.post("/admin/sources", data={"csrf": csrf, "source": source.model_dump_json()}, follow_redirects=False)
    identifier = db.one("SELECT id FROM feed_sources WHERE source_key='external-local'")["id"]
    path = f"/admin/sources/{identifier}/push"
    url = "https://www.raspberrypi.com/news/citizen-science-with-raspberry-pi/"
    entry = {"url": url, "title": "Citizen science with Raspberry Pi"}
    assert client.post(path, json=[entry]).status_code == 403
    headers = {"X-Researcher-CSRF": csrf}
    assert client.post(path, headers=headers, json=[entry]).json()["new"] == 1
    assert client.post(path, headers=headers, json=[{**entry, "url": url + "?utm_source=newsletter"}]).json()["new"] == 0
    assert db.one("SELECT state FROM feed_items")["state"] == "pending_date"
    assert not db.query("SELECT id FROM usage")


def test_source_without_history_can_be_deleted_and_catalog_does_not_restore_it(client, csrf):
    client.get("/admin/sources").raise_for_status()
    deleted = db.one("SELECT * FROM feed_sources ORDER BY id DESC LIMIT 1")
    path = f"/admin/sources/{deleted['id']}/delete"
    assert path in html.fromstring(client.get("/admin/sources").content).xpath("//form/@action")
    assert client.post(path).status_code == 403
    response = client.post(path, data={"csrf": csrf})
    assert response.status_code == 200 and "信源已删除" in response.text
    assert not db.one("SELECT id FROM feed_sources WHERE source_key=?", (deleted["source_key"],))
    assert db.one("SELECT action FROM feed_source_lifecycle WHERE source_id=?", (deleted["id"],)) == {"action": "delete"}
    replacement = save_source(FeedConfig(key="replacement", name="新信源", kind="external", tier="T2", publisher="replacement"))
    assert replacement > deleted["id"]
    assert not db.query("PRAGMA foreign_key_check")


@pytest.mark.parametrize("history", ["run", "item", "both"])
def test_source_with_history_can_be_archived_and_restored(client, csrf, history):
    source = FeedConfig(key="hailo-archive", name="Hailo 采集验证", kind="external", tier="T1", publisher="hailo", enabled=True)
    identifier = save_source(source)
    path = f"/admin/sources/{identifier}"
    now = db.now()
    if history in {"run", "both"}:
        db.execute("INSERT INTO feed_runs(source_id,started_at) VALUES(?,?)", (identifier, now))
    if history in {"item", "both"}:
        url = "https://hailo.ai/products/ai-accelerators/hailo-10h-ai-accelerator/"
        db.execute("INSERT INTO feed_items(source_id,canonical_url,original_url,title,discovered_at,tier,publisher_key,updated_at) "
                   "VALUES(?,?,?,'Hailo-10H AI Accelerator',?,'T1','hailo',?)", (identifier, url, url, now, now))
    runs = db.query("SELECT * FROM feed_runs WHERE source_id=?", (identifier,))
    items = db.query("SELECT * FROM feed_items WHERE source_id=?", (identifier,))
    forms = html.fromstring(client.get("/admin/sources").content).xpath("//form/@action")
    assert path + "/delete" not in forms and path + "/archive" in forms
    assert client.post(path + "/delete").status_code == 403
    rejected = client.post(path + "/delete", data={"csrf": csrf})
    assert rejected.status_code == 200 and "无法删除。请改用归档" in rejected.text
    assert client.post(path + "/archive").status_code == 403
    assert client.post(path + "/archive", data={"csrf": csrf}).status_code == 200
    assert source.name not in client.get("/admin/sources").text
    archive = client.get("/admin/sources?archived=true")
    assert source.name in archive.text and "归档时间" in archive.text
    assert path + "/restore" in html.fromstring(archive.content).xpath("//form/@action")
    assert client.post(path + "/push", json=[], headers={"X-Researcher-CSRF": csrf}).status_code == 404
    forbidden_edit = client.post("/admin/sources", data={"csrf": csrf, "source_id": identifier, "source": source.model_dump_json()})
    assert "请先恢复已归档信源" in forbidden_edit.text
    if items:
        with db.open_database() as connection:
            assert processing_source(connection, items[0]["id"]) is None
    assert client.post(path + "/restore").status_code == 403
    restored = client.post(path + "/restore", data={"csrf": csrf})
    assert restored.status_code == 200 and "信源已恢复为停用状态" in restored.text
    assert source.name in restored.text and source.name not in client.get("/admin/sources?archived=true").text
    assert db.one("SELECT enabled,archived_at FROM feed_sources WHERE id=?", (identifier,)) == {"enabled": 0, "archived_at": None}
    assert db.query("SELECT action FROM feed_source_lifecycle WHERE source_id=? ORDER BY id", (identifier,)) == [
        {"action": "archive"}, {"action": "restore"},
    ]
    assert db.query("SELECT * FROM feed_runs WHERE source_id=?", (identifier,)) == runs
    assert db.query("SELECT * FROM feed_items WHERE source_id=?", (identifier,)) == items
    assert not db.query("PRAGMA foreign_key_check")


def test_only_enabled_official_sources_supply_publisher_prefixes(client, csrf):
    official = FeedConfig(key="official-hailo", name="Hailo 官方", kind="external", tier="T1", publisher="hailo-official",
                          owner="Hailo", publisherUrlPrefixes=["https://hailo.ai/products/"], enabled=True)
    observer = FeedConfig(key="observed-products", name="观察外部报道", kind="external", tier="T2", publisher="observer", enabled=True)
    official_id, observer_id = save_source(official), save_source(observer)
    headers = {"X-Researcher-CSRF": csrf}
    assert client.post(f"/admin/sources/{official_id}/push", json=[], headers=headers).status_code == 200
    push_path = f"/admin/sources/{observer_id}/push"
    assert client.post(push_path, json=[], headers=headers).status_code == 200

    def push_and_check(product, expected):
        url = f"https://hailo.ai/products/{product}/"
        response = client.post(push_path, headers=headers, json=[{"title": product, "url": url, "published_at": db.now()}])
        assert response.status_code == 200 and response.json()["new"] == 1
        item = db.one("SELECT tier,publisher_key,owner_entity_id FROM feed_items WHERE canonical_url=?", (url,))
        assert (item["tier"], item["publisher_key"]) == expected
        assert bool(item["owner_entity_id"]) == (expected[0] == "T1")

    push_and_check("hailo-10h", ("T1", "hailo-official"))
    assert client.post("/admin/sources", data={"csrf": csrf, "source_id": official_id,
                       "source": official.model_copy(update={"enabled": False}).model_dump_json()}).status_code == 200
    push_and_check("hailo-8", ("T2", "observer"))
    assert client.post(f"/admin/sources/{official_id}/archive", data={"csrf": csrf}).status_code == 200
    push_and_check("hailo-15", ("T2", "observer"))
