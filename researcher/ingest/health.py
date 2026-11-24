from datetime import datetime, timedelta, timezone
from statistics import median

from .. import db
from ..industry import selection
from .collectors import stamp


def cadence(source, now, policy):
    history = db.query("SELECT date(i.published_at) AS day,count(*) AS n FROM feed_observations o JOIN feed_items i ON i.id=o.item_id "
                       "WHERE o.source_id=? AND i.published_at>=? AND i.published_at<=? GROUP BY day ORDER BY day",
                       (source["id"], stamp(now - timedelta(days=policy.cadence_days)), stamp(now)))
    days = [datetime.fromisoformat(row["day"]).date() for row in history]
    count = sum(row["n"] for row, day in zip(history, days) if day >= now.date() - timedelta(days=6))
    interval = next(minutes for minimum, minutes in policy.poll_bands if count >= minimum)
    gap = median((later - earlier).days for earlier, later in zip(days, days[1:])) if len(days) >= 3 else source["cadence_days"]
    latest = days[-1] if days else None
    if not latest and gap:
        previous = db.one("SELECT max(date(i.published_at)) AS day FROM feed_observations o JOIN feed_items i ON i.id=o.item_id WHERE o.source_id=? AND i.published_at<=?", (source["id"], stamp(now)))
        latest = datetime.fromisoformat(previous["day"]).date() if previous["day"] else None
    stale = bool(gap and latest and (now.date() - latest).days > gap * policy.stale_multiplier)
    return interval, gap, stale


def refresh(now=None):
    now = now or datetime.now(timezone.utc)
    policy = selection()
    for source in db.query("SELECT id,cadence_days FROM feed_sources WHERE enabled=1"):
        interval, gap, stale = cadence(source, now, policy)
        db.execute("UPDATE feed_sources SET poll_minutes=?,cadence_days=?,cadence_stale=? WHERE id=?", (interval, gap, int(stale), source["id"]))


def next_poll(source, now, failures=0):
    minutes = source["poll_minutes"] * min(8, 2 ** min(failures, 3))
    return stamp(now + timedelta(minutes=minutes))
