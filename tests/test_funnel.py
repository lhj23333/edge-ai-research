import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from researcher import continuity, db, knowledge, research, station
from researcher.config import DATA, ROOT, Profile, save_profile, settings, write_json
from researcher.ingest import benchmark, briefing, jobs
from researcher.ingest.cluster import Decision, assign, commit, confirmed, correct_assignment, shortlist
from researcher.ingest.collectors import DATE_PROBE_BYTES, Entry, FeedConfig, save_source, stamp
from researcher.ingest.funnel import NamedEntity, Prefilter, Score, Writeup, assessment_row, average_score, evaluate, model_request, parse_assessment, process_item, save_assessment, save_original
from researcher.ingest.gate import body_gate, observe
from researcher.industry import selection
from researcher.providers import BudgetExceeded, Gateway, Source, YieldResearch
from researcher.research_engine import ResearchRun
from researcher.research_models import Gap
from researcher.scheduler import tick
from researcher.service import task_documents
from researcher.worker import claim_task

HAILO_TITLE = "Hailo-10H Edge AI Accelerator"
HAILO_URL = "https://hailo.ai/products/ai-accelerators/hailo-10h-ai-accelerator/"
HAILO_SUMMARY = "Hailo-10H 支持在边缘设备上执行生成式 AI 推理。"
RASPBERRY_TITLE = "Citizen science with Raspberry Pi"
RASPBERRY_SUMMARY = "This Maker Monday, we look at ways environmentalists are using Raspberry Pi to understand the world around them."


@pytest.mark.parametrize(("body", "reason"), [
    ("", "empty_body"),
    ("点击查看原文：" + HAILO_URL, "link_only"),
    (HAILO_TITLE, "short_body"),
    ((HAILO_TITLE + " ") * 30, "low_information"),
])
def test_body_gate_rejects_unusable_extractions(body, reason):
    assert body_gate(body) == reason


def record_prefilter(task_id, item, decision):
    payload = {"title": item["title"], "summary": item["summary"][:400]}
    route, version, messages = model_request("prefilter", payload, Prefilter)
    row = assessment_row(item["id"], "prefilter", version, route, task_id, messages)
    return save_assessment(row, decision)


def test_retry_uses_enabled_observer_and_rechecks_initial_archive_window(tmp_path, served_folder, client, csrf):
    title = "SiMa.ai Expands ONE Platform for Edge AI with MLSoC Modalix"
    (tmp_path / "modalix.html").write_text(
        f"<html><head><title>{title}</title><meta property=\"article:published_time\" content=\"2024-09-09T00:00:00+00:00\"></head></html>",
        encoding="utf-8",
    )
    discovered = datetime.fromisoformat("2026-11-23T11:00:00+00:00")
    modalix = Entry(served_folder + "/modalix.html", title)
    hailo = Entry(served_folder + "/hailo.html", HAILO_TITLE)
    original = FeedConfig(key="sima-press-replay", name="SiMa.ai", kind="rss", tier="T1", publisher="sima-ai", url=served_folder + "/sima.xml", enabled=True)
    observer = original.model_copy(update={"key": "embedded-press-replay", "url": served_folder + "/industry.xml"})
    original_id, observer_id = save_source(original), save_source(observer)
    with db.transaction() as connection:
        for source_id, entries in ((original_id, [modalix]), (observer_id, [modalix, hailo])):
            row = connection.execute("SELECT * FROM feed_sources WHERE id=?", (source_id,)).fetchone()
            observe(connection, row, entries, discovered, selection())
    item = db.one("SELECT id,source_id,state FROM feed_items WHERE canonical_url=?", (modalix.url,))
    assert item["source_id"] == original_id and item["state"] == "pending_date"
    save_source(original.model_copy(update={"enabled": False}), original_id)
    other = jobs.enqueue(poll=False)
    response = client.post(f"/admin/funnel/{item['id']}/retry", data={"csrf": csrf}, follow_redirects=False)
    assert response.status_code == 303
    queued = claim_task(False)
    assert queued["id"] != other
    assert station.decode(queued["job_json"])["item_id"] == item["id"]
    record_prefilter(queued["id"], db.one("SELECT * FROM feed_items WHERE id=?", (item["id"],)),
                     Prefilter(keep=True, reason="公告介绍用于边缘推理的 MLSoC 及其软件平台。"))
    asyncio.run(jobs.perform(queued))
    assert db.one("SELECT status FROM tasks WHERE id=?", (queued["id"],))["status"] == "success"
    assert db.one("SELECT source_id,published_at,state,gate_reason FROM feed_items WHERE id=?", (item["id"],)) == {
        "source_id": original_id, "published_at": "2024-09-09T00:00:00+00:00", "state": "gated_archive", "gate_reason": "before_backfill_window",
    }
    assert db.query("SELECT source_id FROM feed_observations WHERE item_id=? ORDER BY source_id", (item["id"],)) == [
        {"source_id": original_id}, {"source_id": observer_id},
    ]
    assert db.one("SELECT state FROM feed_items WHERE canonical_url=?", (hailo.url,))["state"] == "pending_date"
    assert briefing.content(discovered)["background"] == []
    assert not db.query("SELECT id FROM usage")


@pytest.mark.parametrize("scenario,expected_state,expected_reason,requests", [
    ("rejected", "prefiltered_out", "missing_date", 0),
    ("expired", "gated_archive", "expired", 1),
    ("monitoring_start", "gated_archive", "published_after", 1),
    ("late_metadata", "pending_date", "missing_date", 1),
    ("recent", "discovered", "", 2),
])
def test_pending_date_prefilters_before_http_and_rechecks_publication(tmp_path, served_folder, capsys, scenario, expected_state, expected_reason, requests):
    discovered = datetime.now(timezone.utc).replace(microsecond=0)
    published = discovered - (timedelta(hours=49) if scenario == "expired" else timedelta(hours=1))
    sample_key = "raspberry-pi-citizen-science" if scenario == "rejected" else "hailo-10h"
    sample = next(row for row in benchmark.samples() if row["group"] == sample_key and row["expected"] == (scenario != "rejected"))
    summary = RASPBERRY_SUMMARY if scenario == "rejected" else HAILO_SUMMARY
    metadata = f'<meta property="article:published_time" content="{stamp(published)}">'
    if scenario == "late_metadata":
        metadata = "<!--" + " " * DATE_PROBE_BYTES + "-->" + metadata
    (tmp_path / "candidate.html").write_text(
        f"<html><head><title>{sample['title']}</title>{metadata}</head>"
        f"<body><article><h1>{sample['title']}</h1><p>{summary}</p></article></body></html>", encoding="utf-8",
    )
    url = served_folder + "/candidate.html"
    feed = FeedConfig(key="pending-date", name="待补日期", kind="rss", tier="T2", publisher="pending-date",
                      url=served_folder + "/list.xml", enabled=True,
                      published_after=stamp(published) if scenario == "monitoring_start" else "")
    source_id = save_source(feed)
    policy = selection()
    with db.transaction() as connection:
        connection.execute("UPDATE feed_sources SET cursor_json=? WHERE id=?", (station.encode({"baseline_at": stamp(discovered)}), source_id))
        row = connection.execute("SELECT * FROM feed_sources WHERE id=?", (source_id,)).fetchone()
        observe(connection, row, [Entry(url, sample["title"], summary)], discovered, policy)
    item = db.one("SELECT * FROM feed_items")
    assert item["state"] == "pending_date"
    task_id = jobs.enqueue(poll=False)
    assert claim_task(False)["id"] == task_id
    # 按行业标注预置预筛结果，单独检查后续 HTTP 请求。
    record_prefilter(task_id, item, Prefilter(keep=sample["expected"], reason=sample["note"]))
    if scenario == "recent":
        profile = Profile(id="local-budget", name="隔离预算验证", provider="deepseek", model="deepseek-chat",
                          base_url="https://api.deepseek.com/v1", input_price=10, output_price=10)
        save_profile(profile, "unused-budget-test-key")
        config = settings()
        config["active_profile"] = profile.id
        config["limits"]["daily_cost"] = 0.000001
        write_json("config.json", config)

    async def process():
        async with httpx.AsyncClient(trust_env=False) as client:
            await process_item(task_id, item, client, policy, asyncio.Semaphore(1))

    if scenario == "recent":
        with pytest.raises(BudgetExceeded, match="每日总费用安全红线"):
            asyncio.run(process())
    else:
        asyncio.run(process())
    assert capsys.readouterr().err.count('"GET /candidate.html HTTP/1.1" 200') == requests
    result = db.one("SELECT state,gate_reason,published_at,version_id,next_attempt_at FROM feed_items WHERE id=?", (item["id"],))
    assert (result["state"], result["gate_reason"]) == (expected_state, expected_reason)
    assert result["published_at"] == (stamp(published) if scenario in {"expired", "monitoring_start", "recent"} else None)
    assert bool(result["next_attempt_at"]) == (scenario == "late_metadata")
    stages = db.query("SELECT stage,status FROM item_evaluations WHERE item_id=? ORDER BY stage", (item["id"],))
    assert stages[0] == {"stage": "prefilter", "status": "success"}
    if scenario == "recent":
        assert result["version_id"]
        assert stages[1:] == [{"stage": stage, "status": "pending"} for stage in ("score_1", "score_2")]
    else:
        assert result["version_id"] is None and len(stages) == 1
    assert not db.query("SELECT id FROM usage")
    assert not briefing.content(discovered)["events"]


def test_conditional_fetch_pins_the_version_used_by_the_task(tmp_path, served_folder):
    published = datetime.fromisoformat("2026-11-23T10:26:34+00:00")
    detail = tmp_path / "citizen-science.txt"
    detail.write_text(RASPBERRY_SUMMARY, encoding="utf-8")
    url = served_folder + "/" + detail.name
    feed = FeedConfig(key="raspberry-replay", name="Raspberry Pi", kind="rss", tier="T1.5", publisher="raspberry-pi", url=served_folder + "/news.xml", enabled=True)
    source_id = save_source(feed)
    policy = selection()
    with db.transaction() as connection:
        source = connection.execute("SELECT * FROM feed_sources WHERE id=?", (source_id,)).fetchone()
        cursor, _, _ = observe(connection, source, [], published, policy)
        connection.execute("UPDATE feed_sources SET cursor_json=? WHERE id=?", (station.encode(cursor), source_id))
        source = dict(connection.execute("SELECT * FROM feed_sources WHERE id=?", (source_id,)).fetchone())
        observe(connection, source, [Entry(url, RASPBERRY_TITLE, published_at=published)], published + timedelta(minutes=1), policy)
    task = jobs.enqueue(poll=False)

    async def replay():
        async with httpx.AsyncClient(trust_env=False) as client:
            resource = await knowledge.get_resource(client, url, allowed_hosts={"127.0.0.1"})
            document, first_version, content = knowledge.archive_resource(resource, url, None, stamp(published), fallback_title=RASPBERRY_TITLE)
            check = db.one("SELECT * FROM source_checks WHERE document_id=?", (document,))
            unchanged = await knowledge.get_resource(client, url, checked=check, allowed_hosts={"127.0.0.1"})
            assert unchanged.status == 304 and content
            item = db.one("SELECT * FROM feed_items WHERE canonical_url=?", (url,))
            assert await save_original(item, source, task, client, None, policy) == content
            detail.write_text(RASPBERRY_SUMMARY + " Raspberry Pi also supports sensors for temperature, humidity and water quality.", encoding="utf-8")
            resource = await knowledge.get_resource(client, url, allowed_hosts={"127.0.0.1"})
            _, later_version, _ = knowledge.archive_resource(resource, url, None, stamp(published), fallback_title=RASPBERRY_TITLE)
            assert later_version != first_version
            assert task_documents(task)[0]["version_id"] == first_version

    asyncio.run(replay())


def test_independent_votes_and_prompt_version():
    policy = selection()
    first = Score(score=72, reason="产品资料说明了端侧生成式推理能力。")
    second = Score(score=66, reason="厂商资料需要补充独立的测试条件。")
    single = average_score(first, second)
    assert single == 69 and policy.threshold("T1", "厂商公告") <= single
    assert policy.threshold("T1", "开发者更新") > single
    assert policy.threshold("T2", "垂直媒体") > single
    one = model_request("score_1", {"title": HAILO_TITLE}, Score)
    two = model_request("score_2", {"title": HAILO_TITLE}, Score)
    assert one == two and one[0] == "score"
    source = save_source(FeedConfig(key="hailo", name="Hailo", kind="external", tier="T1", publisher="hailo"))
    item = db.execute("INSERT INTO feed_items(source_id,canonical_url,original_url,title,discovered_at,tier,publisher_key,updated_at) VALUES(?,?,?,?,?,'T1','hailo',?)", (source, HAILO_URL, HAILO_URL, HAILO_TITLE, db.now(), db.now()))
    task = jobs.enqueue(poll=False)
    first_call = assessment_row(item, "score_1", one[1], one[0], task, one[2])
    assert first_call == assessment_row(item, "score_1", one[1], one[0], task, one[2])
    assert first_call["id"] != assessment_row(item, "score_2", two[1], two[0], task, two[2])["id"]
    assert Prefilter(keep=False, reason="树莓派环境监测入门缺少推理加速器进展。").keep is False


def test_event_clustering_keeps_one_representative_and_counts_publishers():
    date = datetime.fromisoformat("2025-07-23T04:00:00+00:00")
    policy = selection()
    ids = []
    for key, tier, url in [("zhiding", "T2", "https://ai.zhiding.cn/2025/0723/3169328.shtml"), ("hailo", "T1", HAILO_URL)]:
        source = save_source(FeedConfig(key=key, name=key, kind="external", tier=tier, publisher=key, enabled=True))
        document, version = station.save_document(Source(HAILO_TITLE, url, HAILO_SUMMARY, "公开资料", stamp(date)), None)
        ids.append(db.execute("INSERT INTO feed_items(source_id,canonical_url,original_url,title,published_at,discovered_at,tier,publisher_key,document_id,version_id,score,state,updated_at) "
                              "VALUES(?,?,?,?,?,?,?,?,?,?,80,'selected',?)", (source, url, url, HAILO_TITLE, stamp(date), stamp(date), tier, key, document, version, stamp(date))))
    items = [db.one("SELECT * FROM feed_items WHERE id=?", (identifier,)) for identifier in ids]
    summary = Writeup(title=HAILO_TITLE, summary=HAILO_SUMMARY, importance_reason="公开披露边缘生成式推理能力。", event_type="产品发布")
    first = Decision(relation="new_event", confidence="high", event_id=None, reason="首次记录发布")
    event_id = commit(items[0], summary, first, [], False, date, policy)
    tentative = Decision(relation="same_event", confidence="uncertain", event_id=event_id, reason="媒体资料缺少官方发布日期")
    assert confirmed(tentative).relation == "new_event"
    reviewed = confirmed(tentative.model_copy(update={"confidence": "high", "reason": "两份材料均指向 Hailo-10H 的首发"}))
    assert commit(items[1], summary, reviewed, [{"id": event_id, "similarity": 0.89}], True, date, policy) == event_id
    assert commit(items[1], summary, reviewed, [], True, date, policy) == event_id
    assert db.one("SELECT representative_document_id,independent_source_count,first_party_confirmed FROM events WHERE id=?", (event_id,)) == {"representative_document_id": items[1]["document_id"], "independent_source_count": 2, "first_party_confirmed": 1}
    assert briefing.compose(date)["events"][0]["id"] == event_id
    assert briefing.compose(date + timedelta(hours=24))["events"] == []
    assert db.one("SELECT heat_score FROM events WHERE id=?", (event_id,))["heat_score"] == pytest.approx(1.0)
    assert db.one("SELECT count(*) AS n FROM usage")["n"] == 0
    assert db.one("SELECT count(*) AS n FROM event_cluster_decisions")["n"] == 2


def test_cluster_correction_moves_only_associations_supported_by_the_moved_report(client, csrf, topic):
    for name, question in ((topic["title"], topic["question"]), ("SiMa.ai Palette 3.0", "Palette 3.0 对边缘推理的软件部署有何影响？")):
        response = client.post("/watchlist/add", data={"csrf": csrf, "name": name, "kind": "专题", "question": question}, follow_redirects=False)
        assert response.status_code == 303
    palette_topic = db.one("SELECT id FROM topics WHERE title=?", ("SiMa.ai Palette 3.0",))["id"]
    source = save_source(FeedConfig(key="industry-replay", name="边缘推理资料", kind="external", tier="T2", publisher="industry-replay", enabled=True))
    hailo = NamedEntity(kind="厂商", name="Hailo")
    sima = NamedEntity(kind="厂商", name="SiMa.ai")
    policy = selection()
    new = Decision(relation="new_event", confidence="high", event_id=None, reason="首次记录")

    def select_report(url, title, content, date):
        document, version = station.save_document(Source(title, url, content, "公开资料", stamp(date)), None)
        item = db.execute("INSERT INTO feed_items(source_id,canonical_url,original_url,title,published_at,discovered_at,tier,publisher_key,document_id,version_id,score,state,updated_at) "
                          "VALUES(?,?,?,?,?,?,?,?,?,?,90,?,?)", (source, url, url, title, stamp(date), stamp(date), "T2", httpx.URL(url).host, document, version, "selected", stamp(date)))
        return db.one("SELECT * FROM feed_items WHERE id=?", (item,))

    original_date = datetime.fromisoformat("2025-07-23T04:00:00+00:00")
    original = select_report(HAILO_URL, HAILO_TITLE, HAILO_SUMMARY, original_date)
    summary = Writeup(title=HAILO_TITLE, summary=HAILO_SUMMARY, importance_reason="公布边缘生成式推理能力", event_type="产品发布", entities=[hailo], topic_ids=[topic["id"]])
    original_event = commit(original, summary, new, [], False, original_date, policy)
    palette_date = datetime.fromisoformat("2026-11-17T00:00:00+00:00")
    palette_url = "https://sima.ai/press-release/industry-first-agentic-software-suite-palette-3-0/"
    palette_summary = Writeup(title="SiMa.ai Palette 3.0", summary="SiMa.ai 发布 Palette 3.0 软件套件。", importance_reason="更新边缘推理软件开发工具", event_type="软件更新", entities=[sima], topic_ids=[topic["id"], palette_topic])
    moved = select_report(palette_url, palette_summary.title, palette_summary.summary, palette_date)
    same = Decision(relation="same_event", confidence="high", event_id=original_event, reason="待更正归属")
    assert commit(moved, palette_summary, same, [], False, palette_date, policy) == original_event
    claim = station.save_claim(moved["document_id"], moved["version_id"], palette_summary.summary, palette_summary.summary, "公开披露")
    target, _ = station.add_event({"identity_key": "sima-ai|palette|3.0", "event_type": palette_summary.event_type, "title": palette_summary.title,
                                   "what_happened": palette_summary.summary, "why_important": palette_summary.importance_reason}, [claim], [], [])
    raspberry = station.entity("厂商", "Raspberry Pi")
    original_claim = station.save_claim(original["document_id"], original["version_id"], HAILO_SUMMARY, HAILO_SUMMARY, "公开披露")
    station.add_event({"identity_key": "hailo|hailo-10h|raspberry-pi", "event_type": summary.event_type, "title": HAILO_TITLE, "what_happened": HAILO_SUMMARY},
                      [original_claim], [raspberry], [], existing_event_id=original_event)

    correct_assignment(moved["id"], target, "此报道介绍的是 SiMa.ai 软件发布")
    assert db.query("SELECT e.name,r.feed_only FROM event_entities r JOIN entities e ON e.id=r.entity_id WHERE event_id=? ORDER BY e.name", (original_event,)) == [
        {"name": "Hailo", "feed_only": 1}, {"name": "Raspberry Pi", "feed_only": 0},
    ]
    assert db.query("SELECT topic_id FROM event_topics WHERE event_id=?", (original_event,)) == [{"topic_id": topic["id"]}]
    assert db.query("SELECT e.name FROM event_entities r JOIN entities e ON e.id=r.entity_id WHERE event_id=?", (target,)) == [{"name": "SiMa.ai"}]
    assert {row["topic_id"] for row in db.query("SELECT topic_id FROM event_topics WHERE event_id=?", (target,))} == {topic["id"], palette_topic}


def selected_report(source_id, publisher, tier, url, writeup, published, score=90):
    document, version = station.save_document(Source(writeup.title, url, writeup.summary, "公开资料", stamp(published)), None)
    identifier = db.execute("INSERT INTO feed_items(source_id,canonical_url,original_url,title,published_at,discovered_at,tier,publisher_key,document_id,version_id,score,state,updated_at) "
                            "VALUES(?,?,?,?,?,?,?,?,?,?,?,'selected',?)",
                            (source_id, url, url, writeup.title, stamp(published), stamp(published), tier, publisher, document, version, score, stamp(published)))
    return db.one("SELECT * FROM feed_items WHERE id=?", (identifier,))


def test_archived_redirect_body_never_reaches_the_scoring_model():
    now = datetime.now(timezone.utc)
    source = save_source(FeedConfig(key="hailo", name="Hailo", kind="external", tier="T1", publisher="hailo", enabled=True))
    writeup = Writeup(title=HAILO_TITLE, summary="查看原文：" + HAILO_URL, importance_reason="产品原文链接", event_type="产品发布")
    item = selected_report(source, "hailo", "T1", HAILO_URL, writeup, now)
    db.execute("UPDATE feed_items SET state='discovered',score=NULL WHERE id=?", (item["id"],))
    item = db.one("SELECT * FROM feed_items WHERE id=?", (item["id"],))
    task_id = jobs.enqueue(poll=False)
    assert claim_task(False)["id"] == task_id

    async def process():
        async with httpx.AsyncClient(trust_env=False) as client:
            await process_item(task_id, item, client, selection(), asyncio.Semaphore(1))

    asyncio.run(process())
    assert db.one("SELECT state,gate_reason FROM feed_items WHERE id=?", (item["id"],)) == {
        "state": "prefiltered_out", "gate_reason": "link_only",
    }
    assert not db.query("SELECT id FROM item_evaluations")
    assert not db.query("SELECT id FROM model_call_ledger")


def test_automatic_commit_preserves_manually_locked_event(client, csrf):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    policy = selection()
    media = save_source(FeedConfig(key="zhiding", name="至顶网", kind="external", tier="T2", publisher="zhiding", enabled=True))
    official = save_source(FeedConfig(key="hailo", name="Hailo", kind="external", tier="T1", publisher="hailo", enabled=True))
    initial = Writeup(title=HAILO_TITLE, summary=HAILO_SUMMARY, importance_reason="公布边缘生成式推理能力", event_type="产品发布", version="1.0")
    item = selected_report(media, "zhiding", "T2", "https://ai.zhiding.cn/2025/0723/3169328.shtml", initial, now - timedelta(hours=24))
    new = Decision(relation="new_event", confidence="high", event_id=None, reason="首次记录")
    event = commit(item, initial, new, [], False, now, policy)
    response = client.post(f"/admin/funnel/{item['id']}/assign", data={"csrf": csrf, "event_id": event, "reason": "确认行业报道的归属"}, follow_redirects=False)
    assert response.status_code == 303
    fields = "cluster_locked,title,representative_document_id,what_happened,why_important,event_type,product_version"
    locked = db.one(f"SELECT {fields} FROM events WHERE id=?", (event,))
    assert locked["cluster_locked"] == 1 and locked["representative_document_id"] == item["document_id"]

    follow_up = Writeup(title="Hailo-10H 软件更新", summary="Hailo-10H 软件提供生成式 AI 推理工具。", importance_reason="新增软件工具", event_type="软件更新", version="2.0")
    report = selected_report(official, "hailo", "T1", HAILO_URL, follow_up, now, score=99)
    decision = Decision(relation="follow_up", confidence="high", event_id=event, reason="同一产品的软件进展")
    assert commit(report, follow_up, decision, [], False, now, policy) == event
    assert db.one(f"SELECT {fields} FROM events WHERE id=?", (event,)) == locked
    assert db.one("SELECT independent_source_count,heat_score,first_party_confirmed FROM events WHERE id=?", (event,)) == {
        "independent_source_count": 2, "heat_score": pytest.approx(1.5), "first_party_confirmed": 1,
    }
    assert [(row["version_id"], row["relation"]) for row in knowledge.event_documents(event)] == [
        (item["version_id"], "same_event"), (report["version_id"], "follow_up"),
    ]
    assert commit(item, initial, new, [], False, now, policy) == event
    assert db.one("SELECT event_id FROM feed_items WHERE id=?", (item["id"],))["event_id"] == event
    assert db.query("SELECT manual FROM event_cluster_decisions WHERE item_id=? ORDER BY id", (item["id"],)) == [{"manual": 0}, {"manual": 1}]


def test_manual_assignment_of_new_version_preserves_target_fields(client, csrf):
    now = datetime.now(timezone.utc)
    policy = selection()
    official = save_source(FeedConfig(key="hailo", name="Hailo", kind="external", tier="T1", publisher="hailo", enabled=True))
    release = Writeup(title=HAILO_TITLE, summary=HAILO_SUMMARY, importance_reason="公布边缘生成式推理能力", event_type="产品发布", version="1.0")
    update = release.model_copy(update={"title": "Hailo-10H 软件更新", "event_type": "软件更新", "version": "2.0"})
    initial = selected_report(official, "hailo", "T1", HAILO_URL, release, now - timedelta(hours=24))
    later = selected_report(official, "hailo", "T1", HAILO_URL + "?release=2.0", update, now, score=99)
    separate = Decision(relation="new_event", confidence="high", event_id=None, reason="单独记录发布版本")
    target = commit(initial, release, separate, [], False, now, policy)
    former = commit(later, update, separate, [], False, now, policy)
    assert target != former
    fields = "title,what_happened,why_important,event_type,product_version,published_at,representative_document_id"
    protected = db.one(f"SELECT {fields} FROM events WHERE id=?", (target,))

    response = client.post(f"/admin/funnel/{later['id']}/assign", data={"csrf": csrf, "event_id": target, "reason": "将新版记录为后续进展"}, follow_redirects=False)
    assert response.status_code == 303 and "error" not in response.headers["location"]
    assert db.one("SELECT relation,manual,final_event_id FROM event_cluster_decisions WHERE item_id=? ORDER BY id DESC LIMIT 1", (later["id"],)) == {
        "relation": "follow_up", "manual": 1, "final_event_id": target,
    }
    assert db.one(f"SELECT {fields},cluster_locked FROM events WHERE id=?", (target,)) == {**protected, "cluster_locked": 1}
    assert db.one("SELECT cluster_locked FROM events WHERE id=?", (former,))["cluster_locked"] == 1
    assert [(row["version_id"], row["relation"], row["representative"]) for row in knowledge.event_documents(target)] == [
        (initial["version_id"], "new_event", 1), (later["version_id"], "follow_up", 0),
    ]


@pytest.fixture
def cached_bge_m3():
    source = Path(os.environ.get("RESEARCHER_TEST_MODELS_DIR", ROOT / "data/models"))
    model = source / "models--BAAI--bge-m3"
    if not (model / "refs/main").is_file():
        pytest.skip("未准备本地 BGE-M3，可通过 RESEARCHER_TEST_MODELS_DIR 指定模型目录")
    config = settings()
    config["research"]["device"] = "cpu"
    write_json("config.json", config)
    previous_dir = knowledge.MODELS
    local_models = DATA / "models"
    local_models.mkdir(exist_ok=True)
    model_link = local_models / model.name
    if not model_link.exists():
        model_link.symlink_to(model, target_is_directory=True)
    knowledge.MODELS = local_models
    try:
        yield
    finally:
        knowledge.MODELS = previous_dir
        knowledge.embedding_model.cache_clear()


def test_sqlite_vec_recall_with_cached_bge_m3(cached_bge_m3):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    event = db.execute("INSERT INTO events(identity_key,event_type,title,what_happened,why_important,published_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                       (HAILO_URL, "产品发布", HAILO_TITLE, HAILO_SUMMARY, "边缘生成式推理", stamp(now), stamp(now), stamp(now)))
    window = now - timedelta(days=14)
    same = knowledge.recall_events(HAILO_TITLE + "\n" + HAILO_SUMMARY, window, 5)
    distant = knowledge.recall_events(RASPBERRY_TITLE + "\n" + RASPBERRY_SUMMARY, window, 5)
    assert same[0][0] == event and same[0][1] > 0.99
    assert not shortlist([{"id": identifier, "similarity": score} for identifier, score in distant], selection())
    assert not knowledge.recall_events(HAILO_TITLE, now + timedelta(seconds=1), 5)


def test_identical_text_reuses_persisted_embedding(cached_bge_m3):
    text = HAILO_TITLE + "\n" + HAILO_SUMMARY
    first, duplicate = knowledge.cached_embeddings([text, text], knowledge.EVENT_MODEL, "cpu")
    assert first.tobytes() == duplicate.tobytes() and first.size == knowledge.EVENT_DIMENSION
    entry = db.one("SELECT content_hash,value FROM content_cache")
    assert entry == {"content_hash": continuity.text_hash(text), "value": first.tobytes()}
    knowledge.embedding_model.cache_clear()
    cached, = knowledge.cached_embeddings([text], knowledge.EVENT_MODEL, "cpu")
    assert cached.tobytes() == first.tobytes()
    assert knowledge.embedding_model.cache_info().misses == 0


def test_cross_version_cluster_routes_use_local_recall_and_review_only_uncertain(cached_bge_m3):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    policy = selection()
    source = save_source(FeedConfig(key="hailo", name="Hailo", kind="external", tier="T1", publisher="hailo", enabled=True))
    initial = Writeup(title=HAILO_TITLE, summary=HAILO_SUMMARY, importance_reason="公布边缘生成式推理能力", event_type="产品发布", version="1.0")
    first = selected_report(source, "hailo", "T1", HAILO_URL, initial, now)
    event = commit(first, initial, Decision(relation="new_event", confidence="high", event_id=None, reason="首次记录"), [], False, now, policy)
    task_id = jobs.enqueue(poll=False)
    major_update = initial.model_copy(update={"version": "2.0"})

    for relation, confidence, expected_stages in (
        ("follow_up", "high", ["cluster"]),
        ("same_event", "high", ["cluster"]),
        ("same_event", "uncertain", ["cluster", "cluster_review"]),
    ):
        item = selected_report(source, "hailo", "T1", f"{HAILO_URL}?case={relation}-{confidence}", major_update, now)
        calls = []

        async def replay(stage, payload, result_model):
            assert payload["item"]["version"] == "2.0"
            assert payload["candidates"][0]["product_version"] == "1.0"
            assert payload["candidates"][0]["similarity"] >= policy.cluster_similarity
            calls.append(stage)
            route, version, messages = model_request(stage, payload, result_model)
            row = assessment_row(item["id"], stage, version, route, task_id, messages)
            data = {"relation": relation, "confidence": "high" if stage == "cluster_review" else confidence,
                    "event_id": event, "reason": "同一产品的版本进展"}
            save_assessment(row, parse_assessment(row, result_model, station.encode(data)))
            return await evaluate(task_id, item["id"], stage, payload, result_model, asyncio.Semaphore(1))

        assert asyncio.run(assign(item, major_update, replay, policy)) == event
        assert calls == expected_stages
        assert db.one("SELECT relation,reviewed FROM event_cluster_decisions WHERE item_id=?", (item["id"],)) == {
            "relation": "follow_up", "reviewed": int(confidence == "uncertain"),
        }

    distant = Writeup(title=RASPBERRY_TITLE, summary=RASPBERRY_SUMMARY, importance_reason="环境监测实践", event_type="其他")
    item = selected_report(source, "raspberry-pi", "T1", "https://www.raspberrypi.com/news/citizen-science-with-raspberry-pi/", distant, now)
    calls.clear()
    assert asyncio.run(assign(item, distant, replay, policy)) != event
    assert calls == []
    assert not db.query("SELECT id FROM item_evaluations WHERE item_id=?", (item["id"],))
    decision = db.one("SELECT relation,similarity,reviewed FROM event_cluster_decisions WHERE item_id=?", (item["id"],))
    assert decision["relation"] == "new_event" and decision["similarity"] < 0.62 and decision["reviewed"] == 0


def test_scheduler_polls_enabled_sources_and_compiles_daily_briefing(raspberry_feed):
    source_id = save_source(FeedConfig(key="raspberry-pi", name="Raspberry Pi", kind="rss", tier="T1.5", url=raspberry_feed, publisher="raspberry-pi", enabled=True))
    db.execute("UPDATE station_settings SET auto_enabled=1,next_run=? WHERE id=1", (db.now(),))
    tick()
    task = db.one("SELECT * FROM tasks WHERE kind='feed'")
    assert task["automatic"] and task["priority"] < 0
    db.execute("UPDATE tasks SET status='running' WHERE id=?", (task["id"],))
    asyncio.run(jobs.perform(task))
    assert db.one("SELECT status,new_count FROM feed_runs WHERE source_id=?", (source_id,)) == {"status": "success", "new_count": 1}
    assert db.one("SELECT state FROM feed_items")["state"] == "gated_archive"
    assert db.one("SELECT count(*) AS n FROM daily_briefings")["n"] == 1
    saved = db.one("SELECT body,evidence_hash,polished FROM daily_briefings")
    assert RASPBERRY_TITLE in saved["body"] and not saved["polished"]
    assert briefing.compose()["evidence_hash"] == saved["evidence_hash"]
    assert not db.query("SELECT id FROM usage")


def test_queued_manual_work_preempts_a_feed_job(raspberry_feed):
    save_source(FeedConfig(key="raspberry-pi", name="Raspberry Pi", kind="rss", tier="T1.5", url=raspberry_feed, publisher="raspberry-pi", enabled=True))
    feed_task = jobs.enqueue()
    running = claim_task(False)
    assert running["id"] == feed_task
    interactive = db.execute("INSERT INTO tasks(kind,profile_id,profile_fingerprint,provider,model,created_at,priority) VALUES('event_research','','','','',?,10)", (db.now(),))
    assert claim_task(True)["id"] == interactive
    with pytest.raises(YieldResearch):
        asyncio.run(jobs.perform(running))
    assert not db.query("SELECT id FROM feed_runs")


def test_gap_schedules_independently_of_feed_and_yields_to_manual_work(client, csrf, raspberry_feed):
    save_source(FeedConfig(key="raspberry-pi", name="Raspberry Pi", kind="rss", tier="T1.5", url=raspberry_feed, publisher="raspberry-pi", enabled=True))
    profile = Profile(id="glm", name="智谱", provider="zhipu", model="GLM-5.2", base_url="https://open.bigmodel.cn/api/paas/v4",
                      chat_verified=db.now(), input_price=1, output_price=1)
    save_profile(profile, os.urandom(24).hex())
    client.post("/watchlist/add", data={"csrf": csrf, "kind": "厂商", "name": "Hailo"})
    client.post("/watchlist/confirm", data={"csrf": csrf})
    client.post("/watchlist/automation", data={"csrf": csrf, "enabled": "yes", "schedule_time": "09:00"})
    assert db.one("SELECT auto_enabled,watchlist_confirmed,sources_confirmed FROM station_settings") == {
        "auto_enabled": 1, "watchlist_confirmed": 1, "sources_confirmed": 1,
    }

    task_id = research.enqueue("interactive", {"main_question": "核查 Hailo-10H 的原文参数"})
    assert claim_task(True)["id"] == task_id
    config = settings()
    config["limits"]["daily_cost"] = 0.01
    write_json("config.json", config)
    Gateway(profile, task_id).reserve("generation", estimate=0.01)
    db.execute("UPDATE tasks SET status='success' WHERE id=?", (task_id,))

    gap = Gap(question="核查 Hailo-10H 的 INT4 工作负载和功耗条件", priority=3, reason="官方公告缺少完整测试口径。",
              completion="找出说明测试环境、量化条件和功耗定义的资料。", target_kind="entity",
              target_id=db.one("SELECT id FROM entities WHERE name='Hailo'")["id"])
    gap_id = research.save_gap(gap, {"id": task_id}, [])
    db.execute("UPDATE research_gaps SET automatic_allowed=1 WHERE id=?", (gap_id,))
    tick()
    assert db.one("SELECT kind FROM tasks WHERE status='queued'")["kind"] == "feed"
    assert db.one("SELECT task_id FROM research_gaps WHERE id=?", (gap_id,))["task_id"] is None

    config["limits"].update(daily_cost=None, daily_requests=config["research"]["followup_requests"])
    write_json("config.json", config)
    tick()
    assert db.one("SELECT task_id FROM research_gaps WHERE id=?", (gap_id,))["task_id"] is None
    config["limits"]["daily_requests"] += 1
    config["research"]["automatic_daily_requests"] = config["research"]["followup_requests"] - 1
    write_json("config.json", config)
    tick()
    assert db.one("SELECT task_id FROM research_gaps WHERE id=?", (gap_id,))["task_id"] is None
    config["research"]["automatic_daily_requests"] += 1
    write_json("config.json", config)
    tick()
    running = claim_task(False)
    assert running["kind"] == "followup" and running["id"] == db.one("SELECT task_id FROM research_gaps WHERE id=?", (gap_id,))["task_id"]
    run = ResearchRun(running)
    assert run.scope == {"topics": [], "entities": [{"id": gap.target_id, "kind": "厂商", "name": "Hailo"}]}
    manual = research.enqueue("interactive", {"main_question": "核查本地资料中的软件限制"})
    assert claim_task(True)["id"] == manual
    with pytest.raises(YieldResearch):
        asyncio.run(run.recall({}))
