import asyncio
import time
from datetime import datetime, timedelta, timezone

import httpx

from .. import db, station
from ..config import settings
from ..industry import selection
from ..providers import BudgetExceeded, Cancelled, ResearchError, yield_to_manual
from . import briefing, health
from .collectors import collect, ensure_catalog, parse_date, push_entries, source_config, stamp
from .funnel import ITEM_ERRORS, item_failure, process_item
from .gate import ELIGIBLE_SOURCE, observe, processing_source

READY_ITEMS = """
 FROM feed_items i WHERE i.state IN ('pending_date','discovered','selected')
   AND (i.next_attempt_at IS NULL OR i.next_attempt_at<=?)
   AND EXISTS (SELECT 1 FROM feed_sources s WHERE
""" + ELIGIBLE_SOURCE + ")"
ARCHIVE_REASONS = {"initial_backfill", "expired", "before_backfill_window", "published_after"}


def due_sources(checked):
    return db.query("SELECT id FROM feed_sources WHERE enabled=1 AND kind!='external' AND (next_poll_at IS NULL OR next_poll_at<=?) ORDER BY next_poll_at,id", (checked,))


def has_work(checked):
    return bool(due_sources(checked) or (not db.one("SELECT value FROM runtime WHERE name='feed_budget_until' AND value>?", (checked,))
                                       and db.one("SELECT i.id" + READY_ITEMS + " LIMIT 1", (checked,))))


def enqueue(*, automatic=False, poll=True):
    ensure_catalog()
    created = db.now()
    sources = due_sources(created) if automatic else db.query("SELECT id FROM feed_sources WHERE enabled=1 AND kind!='external' ORDER BY id")
    ids = [row["id"] for row in sources] if poll else []
    if poll and not ids and not db.one("SELECT i.id" + READY_ITEMS + " LIMIT 1", (created,)):
        if automatic:
            return None
        raise ValueError("请在信源管理中启用采集源，或提交一批待处理条目")
    request_limit = settings()["research"]["ingest_requests"]
    with db.transaction() as connection:
        return queue_task(connection, ids, poll, automatic, request_limit)


def queue_task(connection, source_ids, poll, automatic, request_limit, item_id=None):
    active = connection.execute("SELECT id FROM tasks WHERE kind='feed' AND automatic=? AND cancel_requested=0 "
                                "AND json_extract(job_json,'$.item_id') IS ? AND status IN ('queued','running') "
                                "AND (? IS NOT NULL OR (? AND json_extract(job_json,'$.poll')=1) OR (NOT ? AND status='queued')) "
                                "ORDER BY id LIMIT 1", (int(automatic), item_id, item_id, int(poll), int(poll))).fetchone()
    if active:
        return active[0]
    job = {"source_ids": source_ids, "poll": poll}
    prompt, priority = "受控信源采集与漏斗处理", -10 if automatic else -5
    if item_id is not None:
        job["item_id"] = item_id
        prompt, priority = f"继续处理采集条目 #{item_id}", -4
    return connection.execute("INSERT INTO tasks(kind,status,prompt,profile_id,profile_fingerprint,provider,model,created_at,job_json,automatic,priority,request_limit) "
                              "VALUES('feed','queued',?,'','','','',?,?,?,?,?)",
                              (prompt, db.now(), station.encode(job), int(automatic), priority, request_limit)).lastrowid


def begin_run(source, task_id=None):
    with db.transaction() as connection:
        current = connection.execute("SELECT id FROM feed_sources WHERE id=? AND revision=? AND enabled=1 AND archived_at IS NULL",
                                     (source["id"], source["revision"])).fetchone()
        if not current:
            return None
        if task_id is not None:
            existing = connection.execute("SELECT * FROM feed_runs WHERE task_id=? AND source_id=?", (task_id, source["id"])).fetchone()
            if existing:
                return dict(existing)
        identifier = connection.execute("INSERT INTO feed_runs(source_id,task_id,started_at) VALUES(?,?,?)", (source["id"], task_id, db.now())).lastrowid
        return {"id": identifier, "status": "running"}


def finish_run(run, source, entries, resource, started, policy, error=""):
    checked = datetime.now(timezone.utc)
    timestamp = stamp(checked)
    elapsed = int((time.monotonic() - started) * 1000)
    with db.transaction() as connection:
        current = connection.execute("SELECT * FROM feed_sources WHERE id=?", (source["id"],)).fetchone()
        if current["revision"] != source["revision"] or not current["enabled"]:
            connection.execute("UPDATE feed_runs SET status='superseded',finished_at=?,elapsed_ms=? WHERE id=?", (timestamp, elapsed, run["id"]))
            return 0, 0
        if error:
            failures = current["consecutive_failures"] + 1
            connection.execute("UPDATE feed_sources SET next_poll_at=?,consecutive_failures=? WHERE id=?", (health.next_poll(current, checked, failures), failures, source["id"]))
            connection.execute("UPDATE feed_runs SET status='failed',error=?,finished_at=?,elapsed_ms=? WHERE id=?", (error, timestamp, elapsed, run["id"]))
            return 0, 0
        unchanged = resource is not None and resource.status == 304
        cursor, new, gated = observe(connection, current, entries, checked, policy) if not unchanged else (station.decode(current["cursor_json"]), 0, 0)
        following = health.next_poll(current, checked) if resource else None
        connection.execute("UPDATE feed_sources SET cursor_json=?,next_poll_at=?,consecutive_failures=0,last_success_at=? WHERE id=?", (station.encode(cursor), following, timestamp, source["id"]))
        if resource:
            if unchanged:
                connection.execute("UPDATE source_checks SET checked_at=? WHERE feed_source_id=?", (timestamp, source["id"]))
            else:
                connection.execute("INSERT INTO source_checks(feed_source_id,checked_at,final_url,etag,last_modified) VALUES(?,?,?,?,?) "
                                   "ON CONFLICT(feed_source_id) DO UPDATE SET checked_at=excluded.checked_at,final_url=excluded.final_url,etag=excluded.etag,last_modified=excluded.last_modified",
                                   (source["id"], timestamp, resource.url, resource.headers.get("etag", ""), resource.headers.get("last-modified", "")))
        connection.execute("UPDATE feed_runs SET status=?,fetched_count=?,new_count=?,gated_count=?,finished_at=?,elapsed_ms=? WHERE id=?",
                           ("not_modified" if unchanged else "success", len(entries), new, gated, timestamp, elapsed, run["id"]))
        return new, gated


def submit_external(source, payload):
    entries = push_entries(payload, source_config(source))
    policy = selection()
    run = begin_run(source)
    if not run:
        raise ValueError("信源已停用、归档、删除或更新，请刷新后重试")
    new, gated = finish_run(run, source, entries, None, time.monotonic(), policy)
    queued = enqueue(poll=False) if db.one("SELECT i.id" + READY_ITEMS + " LIMIT 1", (db.now(),)) else None
    return {"run_id": run["id"], "new": new, "gated": gated, "task_id": queued}


async def poll(source_id, task_id, client, semaphore, policy):
    async with semaphore:
        yield_to_manual()
        source = db.one("SELECT * FROM feed_sources WHERE id=? AND enabled=1 AND kind!='external'", (source_id,))
        if not source:
            return
        run = begin_run(source, task_id)
        if not run or run["status"] != "running":
            return
        checked = db.one("SELECT * FROM source_checks WHERE feed_source_id=?", (source_id,))
        started = time.monotonic()
        try:
            resource, entries = await collect(client, source_config(source), checked)
        except ITEM_ERRORS as error:
            message = str(error) if isinstance(error, ResearchError) else f"信源抓取失败（{type(error).__name__}）"
            if isinstance(error, httpx.HTTPStatusError):
                message += f" HTTP {error.response.status_code}"
            finish_run(run, source, [], None, started, policy, message)
        else:
            finish_run(run, source, entries, resource, started, policy)


def task_checkpoint(task_id, message):
    task = db.one("SELECT cancel_requested,status FROM tasks WHERE id=?", (task_id,))
    if task["cancel_requested"] or task["status"] != "running":
        raise Cancelled("信源采集已取消")
    yield_to_manual()
    db.execute("UPDATE tasks SET progress=?,heartbeat=? WHERE id=?", (message, db.now(), task_id))


def needs_selection_retry(item, policy):
    return item["state"] == "scored_out" and item["manual_score"] is not None and item["manual_score"] >= policy.threshold(item["tier"], item["category"])


def record_review(connection, item_id, action, reason, *, score=None, event_id=None):
    connection.execute("INSERT INTO feed_manual_reviews(item_id,action,reason,score,event_id,created_at) VALUES(?,?,?,?,?,?)",
                       (item_id, action, reason, score, event_id, db.now(timespec="microseconds")))


def can_release(item):
    return item["state"] == "gated_archive" or (item["state"] == "scored_out" and item["gate_reason"] in ARCHIVE_REASONS)


def retry_item(item_id, reason="", *, release=False):
    from .cluster import assignment_reason, move_assignment, update_event
    policy = selection()
    request_limit = settings()["research"]["ingest_requests"]
    with db.transaction() as connection:
        item = connection.execute("SELECT * FROM feed_items WHERE id=?", (item_id,)).fetchone()
        if not item or item["state"] not in {"pending_date", "discovered", "selected", "scored_out", "gated_archive"}:
            raise ValueError("该条目当前无法继续处理")
        releasing = can_release(item)
        if release and not releasing:
            raise ValueError("该条目已经送回评估或无需人工放行")
        if not processing_source(connection, item_id, releasing=releasing):
            raise ValueError("该条目没有可处理的已启用信源")
        now = datetime.now(timezone.utc)
        published = parse_date(item["published_at"])
        ready_date = published is not None and published <= now
        if item["state"] == "gated_archive" or release:
            if not reason.strip():
                raise ValueError("请填写送回评估的理由")
            record_review(connection, item_id, "release", reason)
            connection.execute("UPDATE feed_items SET state=?,gate_reason='',manual_release=1,manual_score=NULL,manual_reason='' WHERE id=?",
                               ("discovered" if ready_date else "pending_date", item_id))
        elif item["state"] == "scored_out":
            if not needs_selection_retry(item, policy):
                raise ValueError("更正后的评分需要达到入选门槛")
            state = "clustered" if item["event_id"] else "selected" if ready_date else "pending_date"
            if releasing:
                record_review(connection, item_id, "release", reason or item["manual_reason"])
            connection.execute("UPDATE feed_items SET state=?,event_visible_at=?,manual_release=max(manual_release,?),gate_reason='' WHERE id=?",
                               (state, db.now(timespec="microseconds") if item["event_id"] else None, int(releasing), item_id))
            if item["event_id"]:
                if item["manual_target_event_id"] and item["manual_target_event_id"] != item["event_id"]:
                    target = connection.execute("SELECT * FROM events WHERE id=? AND state='active' AND canonical_event_id IS NULL",
                                                (item["manual_target_event_id"],)).fetchone()
                    if not target:
                        raise ValueError("人工指定的目标事件已失效，请重新指定归属")
                    move_assignment(connection, item, target, assignment_reason(connection, item_id), now, policy)
                else:
                    update_event(connection, item["event_id"], now, policy)
        connection.execute("UPDATE research_operations SET status='retry_authorized',updated_at=? WHERE status IN ('unknown','running','invalid','truncated','failed') "
                           "AND EXISTS (SELECT 1 FROM item_evaluations e WHERE e.item_id=? AND e.status IN ('failed','unknown') AND e.task_id=research_operations.task_id AND research_operations.operation_key='feed:'||e.id)", (db.now(), item_id))
        connection.execute("UPDATE item_evaluations SET status='pending',reason='',updated_at=? WHERE item_id=? AND status IN ('failed','unknown')", (db.now(), item_id))
        connection.execute("UPDATE feed_items SET error='',next_attempt_at=NULL,updated_at=? WHERE id=?", (db.now(), item_id))
        return queue_task(connection, [], False, False, request_limit, item_id)


async def perform(task):
    task_id = task["id"]
    policy = selection()
    job = station.decode(task["job_json"])
    item_id = job.get("item_id")
    task_checkpoint(task_id, "正在处理指定条目" if item_id is not None else "正在轮询启用的信源")
    async with httpx.AsyncClient(timeout=httpx.Timeout(30, connect=8), trust_env=False) as client:
        semaphore = asyncio.Semaphore(4)
        outcomes = await asyncio.gather(*(poll(source_id, task_id, client, semaphore, policy) for source_id in job["source_ids"]), return_exceptions=True)
        for outcome in outcomes:
            if isinstance(outcome, BaseException):
                raise outcome
        runs = db.query("SELECT * FROM feed_runs WHERE task_id=? ORDER BY id", (task_id,))
        success = [run for run in runs if run["status"] in {"success", "not_modified"}]
        failures = [run for run in runs if run["status"] == "failed"]
        if runs:
            db.execute("UPDATE station_settings SET last_attempt_at=?,last_success_at=CASE WHEN ? THEN ? ELSE last_success_at END,last_status=?,last_error=? WHERE id=1",
                       (db.now(), bool(success), db.now(), "partial" if failures and success else "failed" if failures else "complete",
                        "；".join(run["error"] for run in failures[:3])))
        paused = task["automatic"] and db.one("SELECT value FROM runtime WHERE name='feed_budget_until' AND value>?", (db.now(),))
        if item_id is not None:
            items = db.query("SELECT i.*" + READY_ITEMS + " AND i.id=?", (db.now(), item_id))
        else:
            items = [] if paused else db.query("SELECT i.*" + READY_ITEMS + " ORDER BY i.state='pending_date',i.published_at DESC,i.id LIMIT 40", (db.now(),))
        count = 0
        budget = ""
        errors = 0
        for item in items:
            task_checkpoint(task_id, f"处理条目 {count + 1}/{len(items)}")
            try:
                await process_item(task_id, item, client, policy, semaphore)
            except BudgetExceeded as error:
                item_failure(item["id"], error)
                budget = str(error)
                if task["automatic"] and "单次任务" not in budget:
                    delay = stamp(datetime.now(timezone.utc) + timedelta(minutes=15))
                    db.execute("INSERT INTO runtime(name,value) VALUES('feed_budget_until',?) ON CONFLICT(name) DO UPDATE SET value=excluded.value", (delay,))
                break
            except ITEM_ERRORS as error:
                item_failure(item["id"], error)
                errors += 1
            count += 1
    if item_id is not None:
        item = db.one("SELECT state,error FROM feed_items WHERE id=?", (item_id,))
        incomplete = not item or item["state"] in {"pending_date", "discovered", "selected"}
        error = budget or (item["error"] if item else "")
        if incomplete and not error:
            error = "条目仍在等待发布日期" if item and item["state"] == "pending_date" else "条目当前无法处理，请检查信源与筛选状态"
        db.execute("UPDATE tasks SET status=?,progress=?,result_json=?,result_text=?,error=?,finished_at=?,heartbeat=? WHERE id=? AND cancel_requested=0",
                   ("failed" if incomplete else "success", "条目尚未处理完成" if incomplete else "指定条目已处理",
                    station.encode({"item_id": item_id, "item_state": item["state"] if item else None, "budget": budget}),
                    f"条目 #{item_id} 已完成本次处理。" if not incomplete else "", error, db.now(), db.now(), task_id))
        return
    issue = briefing.compose()
    await briefing.polish(task_id, issue)
    summary = f"信源检查成功 {len(success)} 项，失败 {len(failures)} 项。新增条目 {sum(run['new_count'] for run in runs)} 条。今日简报有 {len(issue['events'])} 件事件。"
    if errors:
        summary += f" {errors} 条资料待继续处理。"
    if budget:
        summary += " " + budget
    state = "failed" if failures and not success and not count else "success"
    db.execute("UPDATE tasks SET status=?,progress=?,result_json=?,result_text=?,error=?,finished_at=?,heartbeat=? WHERE id=? AND cancel_requested=0",
               (state, "采集已结束", station.encode({"runs": [run["id"] for run in runs], "events": [row["id"] for row in issue["events"]], "budget": budget}),
                summary, "本次所有信源抓取失败" if state == "failed" else "", db.now(), db.now(), task_id))
