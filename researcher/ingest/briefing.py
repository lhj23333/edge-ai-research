from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from .. import db, station
from ..config import settings
from ..industry import selection
from .cluster import refresh_heat, show_heat
from .collectors import stamp


EVENT_FEEDS = """
WITH feed_stats AS (
 SELECT event_id,max(published_at) AS latest_publication,
        max(coalesce(manual_score,score)) AS feed_score
 FROM feed_items WHERE state='clustered' GROUP BY event_id
)
"""


def day_bounds(now, zone):
    local = now.astimezone(ZoneInfo(zone))
    start = datetime.combine(local.date(), time.min, local.tzinfo)
    return local.date().isoformat(), start, start + timedelta(days=1)


def revisions_since(since):
    return db.query("SELECT v.*,t.title AS topic_title,j.topic_id,j.purpose FROM judgments j JOIN judgment_versions v ON v.id=j.current_version_id "
                    "JOIN topics t ON t.id=j.topic_id WHERE v.created_at>=? ORDER BY v.id DESC LIMIT 10", (since,))


def content(now=None):
    now = now or datetime.now(timezone.utc)
    day, start, end = day_bounds(now, settings()["limits"]["timezone"])
    policy = selection()
    cutoff = stamp(now - timedelta(hours=policy.freshness_hours))
    events = db.query(EVENT_FEEDS + "SELECT e.*,f.latest_publication,f.feed_score,d.url AS representative_url FROM feed_stats f "
                      "JOIN events e ON e.id=f.event_id LEFT JOIN documents d ON d.id=e.representative_document_id "
                      "WHERE e.state='active' AND e.background=0 AND f.latest_publication>=? AND f.latest_publication<? "
                      "ORDER BY e.id DESC LIMIT 200",
                      (max(stamp(start), cutoff), stamp(end)))
    show_heat(events, now, policy)
    events.sort(key=lambda row: (row["first_party_confirmed"], row["heat_score"], row["feed_score"], row["id"]), reverse=True)
    background = db.query("SELECT id,title,published_at,canonical_url,tier,gate_reason FROM feed_items WHERE published_at IS NOT NULL AND published_at<=? "
                          "AND ((state='gated_archive' AND gate_reason!='before_backfill_window') OR (state='clustered' AND published_at<? AND NOT EXISTS "
                          "(SELECT 1 FROM feed_items n WHERE n.event_id=feed_items.event_id AND n.state='clustered' AND n.published_at>=?) "
                          "AND id=(SELECT n.id FROM feed_items n JOIN events e ON e.id=n.event_id WHERE n.event_id=feed_items.event_id AND n.state='clustered' "
                          "ORDER BY (n.document_id=e.representative_document_id) DESC,n.published_at DESC,n.id DESC LIMIT 1))) "
                          "ORDER BY published_at DESC,id DESC LIMIT 8", (stamp(now), cutoff, cutoff))
    return {"day": day, "events": events[:15], "background": background, "revisions": revisions_since(stamp(start))}


def compose(now=None):
    now = now or datetime.now(timezone.utc)
    refresh_heat(now, selection())
    issue = content(now)
    db.execute("INSERT INTO daily_briefings(day,created_at,event_ids_json,background_ids_json,revision_ids_json) VALUES(?,?,?,?,?) "
               "ON CONFLICT(day) DO UPDATE SET created_at=excluded.created_at,event_ids_json=excluded.event_ids_json,"
               "background_ids_json=excluded.background_ids_json,revision_ids_json=excluded.revision_ids_json",
               (issue["day"], stamp(now), *(station.encode([row["id"] for row in issue[key]]) for key in ("events", "background", "revisions"))))
    return issue
