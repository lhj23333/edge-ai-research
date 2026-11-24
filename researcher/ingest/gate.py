import asyncio
from calendar import monthrange
from datetime import datetime, timedelta, timezone

import httpx
from lxml import etree

from .. import continuity, db, station
from ..industry import selection
from ..providers import ResearchError
from .collectors import article_date, collect, matches_prefix, parse_date, push_entries, source_config, stamp


OBSERVED_SOURCE = """
 s.enabled=1 AND s.archived_at IS NULL AND s.id IN (
   SELECT i.source_id UNION ALL SELECT o.source_id FROM feed_observations o WHERE o.item_id=i.id
 )
"""
ELIGIBLE_SOURCE = OBSERVED_SOURCE + " AND (s.published_after IS NULL OR i.published_at IS NULL OR i.published_at>s.published_after)"


def processing_source(connection, item_id):
    return connection.execute("SELECT s.* FROM feed_items i CROSS JOIN feed_sources s WHERE i.id=? AND "
                              + ELIGIBLE_SOURCE + " ORDER BY (s.id=i.source_id) DESC,s.id LIMIT 1", (item_id,)).fetchone()


def archive_cutoff(now, months):
    year, month = divmod(now.year * 12 + now.month - 1 - months, 12)
    return now.replace(year=year, month=month + 1, day=min(now.day, monthrange(year, month + 1)[1]))


def gate_date(published, discovered, checked, published_after, is_backfill, policy):
    if published is None:
        return "pending_date", "missing_date"
    if published > checked:
        return "pending_date", "future_date"
    if is_backfill and published < archive_cutoff(discovered, policy.backfill_months):
        return "gated_archive", "before_backfill_window"
    if published_after and published <= published_after:
        return "gated_archive", "published_after"
    # 补全发布日期后仍以首次发现时间判定时效。
    if discovered - published > timedelta(hours=policy.freshness_hours):
        return "gated_archive", "expired"
    if is_backfill:
        return "gated_archive", "initial_backfill"
    return "discovered", ""


def publication_state(connection, item, published, checked, policy):
    cutoff = connection.execute("SELECT CASE WHEN count(*)=count(s.published_after) THEN min(s.published_after) END "
                                "FROM feed_items i CROSS JOIN feed_sources s WHERE i.id=? AND "
                                + OBSERVED_SOURCE, (item["id"],)).fetchone()[0]
    return gate_date(published, parse_date(item["discovered_at"]), checked, parse_date(cutoff), item["is_backfill"], policy)


def publishers(connection):
    candidates = [(prefix, source) for source in connection.execute("SELECT * FROM feed_sources WHERE tier='T1' AND enabled=1 AND archived_at IS NULL")
                  for prefix in source_config(dict(source)).publisherUrlPrefixes]
    return sorted(candidates, key=lambda item: len(item[0]), reverse=True)


def publisher_for(url, source, known_publishers):
    return next((owner for prefix, owner in known_publishers if matches_prefix(url, prefix)), source)


def known_document(connection, canonical, original):
    return connection.execute("""
        SELECT d.id,d.published_at,v.id AS version_id
        FROM documents d LEFT JOIN document_versions v ON v.document_id=d.id
        WHERE d.id=coalesce(
            (SELECT document_id FROM source_aliases WHERE url=?),
            (SELECT document_id FROM source_aliases WHERE url=?),
            (SELECT id FROM documents WHERE url=?),
            (SELECT id FROM documents WHERE url=?)
        )
        ORDER BY (v.content_hash=d.content_hash) DESC,v.fetched_at DESC,v.id DESC LIMIT 1
    """, (canonical, original, canonical, original)).fetchone()


def existing_item(connection, canonical, document):
    direct = connection.execute("SELECT * FROM feed_items WHERE canonical_url=?", (canonical,)).fetchone()
    if direct:
        return direct
    if document:
        return connection.execute("SELECT * FROM feed_items WHERE document_id=? ORDER BY state='duplicate',event_id IS NULL,id LIMIT 1",
                                  (document["id"],)).fetchone()


def observe(connection, source, entries, checked, policy):
    cursor = station.decode(source["cursor_json"])
    first = not cursor.get("baseline_at")
    timestamp = stamp(checked)
    ordered = sorted(entries, key=lambda entry: entry.published_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    candidates = {}
    for entry in ordered:
        candidates.setdefault(continuity.canonical_url(entry.url), entry)
    if first:
        cutoff = archive_cutoff(checked, policy.backfill_months)
        accepted = [url for url, entry in candidates.items() if entry.published_at is None or entry.published_at >= cutoff]
        cursor.update(baseline_at=timestamp, backfill_excluded=sorted(candidates.keys() - set(accepted[:policy.backfill_limit])))
    excluded = set(cursor.get("backfill_excluded", ()))
    known_publishers = publishers(connection)
    new = eligible = 0
    for canonical, entry in candidates.items():
        document = known_document(connection, canonical, entry.url)
        existing = existing_item(connection, canonical, document)
        if existing:
            item_id = existing["id"]
        else:
            if canonical in excluded and not document:
                continue
            owner = publisher_for(canonical, source, known_publishers)
            published = stamp(entry.published_at) if entry.published_at else None
            if document:
                if document["version_id"] is None:
                    raise ResearchError(f"历史文档 #{document['id']} 缺少可引用的版本")
                state, reason = "duplicate", "historical_document"
                published = document["published_at"] or published
            else:
                state, reason = gate_date(entry.published_at, checked, checked, parse_date(source["published_after"]), first, policy)
            item_id = connection.execute(
                "INSERT INTO feed_items(source_id,canonical_url,original_url,title,summary,published_at,discovered_at,is_backfill,state,gate_reason,tier,category,publisher_key,owner_entity_id,document_id,version_id,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (source["id"], canonical, entry.url, entry.title, entry.summary, published, timestamp, int(first), state, reason,
                 owner["tier"], owner["category"], owner["publisher_key"], owner["owner_entity_id"],
                 document["id"] if document else None, document["version_id"] if document else None, timestamp),
            ).lastrowid
            new += 1
            eligible += state == "discovered"
        connection.execute("INSERT OR IGNORE INTO feed_observations(source_id,item_id,seen_at) VALUES(?,?,?)", (source["id"], item_id, timestamp))
        if existing and (existing["state"] == "pending_date" or existing["gate_reason"] == "published_after"):
            published = parse_date(existing["published_at"]) or entry.published_at
            state, reason = publication_state(connection, existing, published, checked, policy)
            published_at = stamp(published) if published else None
            if (published_at, state, reason) != (existing["published_at"], existing["state"], existing["gate_reason"]):
                connection.execute("UPDATE feed_items SET published_at=?,state=?,gate_reason=?,error='',next_attempt_at=NULL,updated_at=? WHERE id=?",
                                   (published_at, state, reason, timestamp, item_id))
                eligible += state == "discovered"
    return cursor, new, len(entries) - eligible


async def preview(source, payload=None, current=None):
    policy = selection()
    checked = datetime.now(timezone.utc)
    cursor = station.decode(current["cursor_json"]) if current else {}
    first = not cursor.get("baseline_at")
    excluded = set(cursor.get("backfill_excluded", ()))
    async with httpx.AsyncClient(timeout=httpx.Timeout(15, connect=5), follow_redirects=False, trust_env=False) as client:
        if source.kind == "external":
            entries = push_entries(payload, source)
        else:
            _, entries = await collect(client, source)
        semaphore = asyncio.Semaphore(4)

        async def inspect(entry):
            canonical = continuity.canonical_url(entry.url)
            published = entry.published_at
            error = ""
            with db.open_database() as connection:
                document = known_document(connection, canonical, entry.url)
                existing = existing_item(connection, canonical, document)
            if existing or (canonical in excluded and not document):
                state, reason = "duplicate", "known_url"
            elif document:
                state, reason = "duplicate", "historical_document"
                published = parse_date(document["published_at"]) or published
            else:
                try:
                    if not published:
                        async with semaphore:
                            published = await article_date(client, source, entry.url)
                except (httpx.HTTPError, OSError, ValueError, etree.LxmlError, ResearchError) as exc:
                    error = str(exc) if isinstance(exc, ResearchError) else type(exc).__name__
                state, reason = gate_date(published, checked, checked, parse_date(source.published_after), first, policy)
            return {"url": canonical, "title": entry.title, "published_at": stamp(published) if published else None,
                    "state": state, "gate_reason": reason, "error": error}

        return await asyncio.gather(*(inspect(entry) for entry in entries[:20]))
