from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from . import db
from .config import get_profile, settings


def next_run(schedule, clock, weekday, zone, after=None):
    if schedule == "manual":
        return None
    local = (after or datetime.now(timezone.utc)).astimezone(ZoneInfo(zone))
    hour, minute = (int(item) for item in clock.split(":"))
    candidate = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if schedule == "weekly":
        candidate += timedelta(days=(weekday - candidate.weekday()) % 7)
    if candidate <= local:
        candidate += timedelta(days=7 if schedule == "weekly" else 1)
    return candidate.astimezone(timezone.utc).isoformat(timespec="seconds")


def tick():
    from .service import enqueue
    from .station import enqueue as enqueue_station
    for topic in db.query("SELECT * FROM topics WHERE research_state='active' AND schedule!='manual' AND next_run<=?", (db.now(),)):
        try:
            profile = get_profile()
            if not profile.search_verified:
                raise ValueError("当前模型未完成联网测试，自动研究暂停")
            enqueue(topic["id"], "research", profile_id=profile.id, schedule_key=f"{topic['id']}:{topic['next_run']}")
        except ValueError as error:
            db.execute("INSERT INTO runtime(name,value) VALUES('scheduler_error',?) ON CONFLICT(name) DO UPDATE SET value=excluded.value", (str(error),))
            continue
        following = next_run(topic["schedule"], topic["schedule_time"], topic["weekday"], settings()["limits"]["timezone"])
        db.execute("UPDATE topics SET next_run=? WHERE id=?", (following, topic["id"]))
        db.execute("DELETE FROM runtime WHERE name='scheduler_error'")
    station = db.one("SELECT * FROM station_settings WHERE id=1")
    if station and station["auto_enabled"] and station["next_run"] and station["next_run"] <= db.now():
        try:
            enqueue_station("scan", schedule_key="station:" + station["next_run"], automatic=True)
        except ValueError as error:
            db.execute("INSERT INTO runtime(name,value) VALUES('scheduler_error',?) ON CONFLICT(name) DO UPDATE SET value=excluded.value", (str(error),))
            return
        following = next_run("daily", station["schedule_time"], 0, settings()["limits"]["timezone"])
        db.execute("UPDATE station_settings SET next_run=? WHERE id=1", (following,))
    from .research import schedule_followups
    try:
        schedule_followups()
    except ValueError as error:
        db.execute("INSERT INTO runtime(name,value) VALUES('scheduler_error',?) ON CONFLICT(name) DO UPDATE SET value=excluded.value", (str(error),))
