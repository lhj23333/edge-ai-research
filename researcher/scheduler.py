from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from . import db, research
from .config import Limits, settings
from .providers import cost_windows


def next_run(clock, zone, after=None):
    return next_topic_run("daily", clock, 0, zone, after)


def next_topic_run(frequency, clock, weekday, zone, after=None):
    local = (after or datetime.now(timezone.utc)).astimezone(ZoneInfo(zone))
    hour, minute = (int(item) for item in clock.split(":"))
    candidate = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if frequency == "weekly":
        candidate += timedelta(days=(weekday - candidate.weekday()) % 7)
    if candidate <= local:
        candidate += timedelta(days=7 if frequency == "weekly" else 1)
    return candidate.astimezone(timezone.utc).isoformat(timespec="seconds")


def pause_topic_schedule(connection, topic_id):
    connection.execute("UPDATE topic_research_schedules SET next_run=NULL WHERE topic_id=?", (topic_id,))
    queued = connection.execute("SELECT id,session_id FROM tasks WHERE topic_id=? AND schedule_key LIKE 'topic-revision:%' AND status='queued'", (topic_id,)).fetchall()
    timestamp = db.now()
    for task in queued:
        connection.execute("UPDATE tasks SET status='cancelled',finished_at=?,progress='计划已停用' WHERE id=?", (timestamp, task["id"]))
        connection.execute("UPDATE research_sessions SET status='cancelled',updated_at=? WHERE id=?", (timestamp, task["session_id"]))


def tick():
    from .ingest import briefing, health, jobs
    station = db.one("SELECT * FROM station_settings WHERE id=1")
    if not station["auto_enabled"]:
        return
    checked = db.now()
    if not db.one("SELECT id FROM tasks WHERE kind='feed' AND status IN ('queued','running') LIMIT 1") and jobs.has_work(checked):
        jobs.enqueue(automatic=True)
    if station["next_run"] and station["next_run"] <= checked:
        health.refresh()
        briefing.compose()
        following = next_run(station["schedule_time"], settings()["limits"]["timezone"])
        db.execute("UPDATE station_settings SET next_run=? WHERE id=1", (following,))
    try:
        schedule_directed_research()
    except ValueError as error:
        record_error(error)
    schedule_event_research()


def record_error(error):
    db.execute("INSERT INTO runtime(name,value) VALUES('scheduler_error',?) ON CONFLICT(name) DO UPDATE SET value=excluded.value WHERE runtime.value!=excluded.value", (str(error),))


def schedule_directed_research():
    config = settings()
    limits = Limits(**config["limits"])
    policy = config["research"]
    current = datetime.now(timezone.utc)
    checked = current.isoformat(timespec="seconds")
    local = current.astimezone(ZoneInfo(limits.timezone))
    day = local.date().isoformat()
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat(timespec="seconds")
    with db.transaction() as connection:
        if not connection.execute("SELECT id FROM station_settings WHERE id=1 AND auto_enabled=1 AND watchlist_confirmed=1 AND sources_confirmed=1").fetchone():
            return
        if connection.execute("SELECT id FROM tasks WHERE automatic=1 AND session_id IS NOT NULL AND status IN ('queued','running') LIMIT 1").fetchone():
            return

        gap = connection.execute("""
            SELECT g.* FROM research_gaps g
            WHERE g.status='open' AND g.automatic_allowed=1 AND g.next_check_at<=?
              AND (g.target_kind='domain' OR (g.target_kind='topic'
                   AND EXISTS (SELECT 1 FROM topics WHERE id=g.target_id AND research_state='active')
                   AND NOT EXISTS (SELECT 1 FROM tasks WHERE topic_id=g.target_id AND status IN ('queued','running')))
                   OR (g.target_kind='entity'
                   AND EXISTS (SELECT 1 FROM entities WHERE id=g.target_id AND followed=1 AND state='active')))
            ORDER BY g.priority DESC,g.attempts,g.id LIMIT 1
        """, (checked,)).fetchone()
        followups = connection.execute("SELECT count(*) FROM tasks WHERE kind='followup' AND automatic=1 AND julianday(created_at)>=julianday(?)", (midnight,)).fetchone()[0] if gap else 0
        plan = connection.execute("""
            SELECT t.id,t.title,t.question,s.frequency,s.schedule_time,s.weekday,s.next_run
            FROM topic_research_schedules s JOIN topics t ON s.topic_id=t.id
            WHERE s.next_run<=? AND t.research_state!='archived'
              AND NOT EXISTS (SELECT 1 FROM tasks WHERE topic_id=t.id AND status IN ('queued','running'))
            ORDER BY s.next_run,t.id LIMIT 1
        """, (checked,)).fetchone()
        if not gap and not plan:
            return

        used = connection.execute("SELECT count(*) AS total,coalesce(sum(t.automatic),0) AS automatic FROM usage u JOIN tasks t ON t.id=u.task_id WHERE u.day=?", (day,)).fetchone()
        remaining = min(limits.daily_requests - used["total"], policy["automatic_daily_requests"] - used["automatic"])
        if any(spent >= limit for _, limit, spent in cost_windows(connection, limits, current, day)):
            return
        if gap and followups < policy["followups_per_day"] and remaining >= policy["followup_requests"]:
            research.enqueue_gap(gap, connection, automatic=True)
        elif plan and remaining >= policy["revise_requests"]:
            job = research.topic_job(plan)
            job["main_question"] = f"基于本机已归档资料，检查《{plan['title']}》的行业竞争和部署决策是否需要更新。研究范围：{plan['question']}"
            research.enqueue("revise", job, schedule_key=f"topic-revision:{plan['id']}:{plan['next_run']}",
                             automatic=True, connection=connection)
            following = next_topic_run(plan["frequency"], plan["schedule_time"], plan["weekday"], limits.timezone, current)
            connection.execute("UPDATE topic_research_schedules SET next_run=? WHERE topic_id=?", (following, plan["id"]))
        else:
            return
        connection.execute("DELETE FROM runtime WHERE name='scheduler_error'")


def schedule_event_research():
    from . import station
    from .industry import selection
    from .research import enqueue_event
    config = settings()
    research = config["research"]
    if not research["automatic_event_research"] or db.one("SELECT id FROM tasks WHERE status IN ('queued','running') LIMIT 1"):
        return
    current = datetime.now(timezone.utc)
    local = current.astimezone(ZoneInfo(config["limits"]["timezone"]))
    day = local.date().isoformat()
    allowance = research["followup_requests"]
    used = db.one("SELECT count(*) AS total,coalesce(sum(t.automatic),0) AS automatic FROM usage u JOIN tasks t ON t.id=u.task_id WHERE u.day=?", (day,))
    if used["total"] + allowance > config["limits"]["daily_requests"] or used["automatic"] + allowance > research["automatic_daily_requests"]:
        return
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat(timespec="seconds")
    count = db.one("SELECT count(*) AS n FROM tasks WHERE kind='event_research' AND automatic=1 AND created_at>=?", (midnight,))["n"]
    if count >= research["followups_per_day"]:
        return
    policy = selection()
    since = (current - timedelta(hours=policy.freshness_hours)).isoformat(timespec="seconds")
    candidate = db.one("SELECT e.id FROM events e JOIN feed_items i ON i.event_id=e.id WHERE i.state='clustered' AND i.tier='T1' "
                       "AND i.published_at>=? AND coalesce(i.manual_score,i.score)>=? AND e.state='active' AND e.canonical_event_id IS NULL "
                       "AND e.event_type IN (SELECT value FROM json_each(?)) "
                       "AND NOT EXISTS(SELECT 1 FROM tasks t WHERE t.kind='event_research' AND json_extract(t.job_json,'$.target_id')=e.id) "
                       "ORDER BY coalesce(i.manual_score,i.score) DESC,e.heat_score DESC,e.id DESC LIMIT 1",
                       (since, policy.auto_research_score, station.encode(policy.auto_research_types)))
    if candidate:
        try:
            enqueue_event(candidate["id"], automatic=True)
        except ValueError as error:
            record_error(error)
        else:
            db.execute("DELETE FROM runtime WHERE name='scheduler_error'")
