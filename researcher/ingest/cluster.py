import asyncio
from datetime import datetime, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, model_validator

from .. import continuity, db, knowledge, station
from ..industry import selection
from ..providers import yield_to_manual
from .collectors import parse_date, stamp
from .gate import processing_source


class Decision(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reason: str = Field(min_length=1, max_length=500)
    relation: Literal["same_event", "follow_up", "new_event"]
    confidence: Literal["high", "uncertain"]
    event_id: int | None = Field(ge=1)

    @model_validator(mode="after")
    def relation_target(self, info: ValidationInfo):
        if (self.relation == "new_event") != (self.event_id is None):
            raise ValueError("只有归入已有事件的判定可以包含事件编号")
        if info.context and self.event_id is not None and self.event_id not in {item["id"] for item in info.context["candidates"]}:
            raise ValueError("事件判定引用了未提供的候选编号")
        return self


def candidates_for(summary, now, policy):
    ranked = knowledge.recall_events(summary.title + "\n" + summary.summary, now - timedelta(days=policy.cluster_days), policy.cluster_top)
    candidates = []
    for event_id, similarity in ranked:
        event = db.one("SELECT e.id,e.title,e.what_happened,e.event_type,e.published_at,e.product_version,d.url "
                       "FROM events e LEFT JOIN documents d ON d.id=e.representative_document_id WHERE e.id=? AND e.state='active' AND e.canonical_event_id IS NULL", (event_id,))
        if event:
            candidates.append({**event, "similarity": similarity})
    return candidates


def shortlist(candidates, policy):
    return [candidate for candidate in candidates if candidate["similarity"] >= policy.cluster_similarity]


def confirmed(decision):
    if decision.confidence == "uncertain":
        return Decision(relation="new_event", confidence="high", event_id=None, reason="复核后仍缺少可确认的同一事件依据。" + decision.reason[:420])
    return decision


def metrics_for(rows, now, policy):
    cutoff = now - timedelta(hours=policy.freshness_hours)
    latest = {}
    for row in rows:
        published = parse_date(row["published_at"])
        if published and cutoff <= published <= now:
            key = row["publisher_key"]
            latest[key] = max(latest.get(key, cutoff), published)
    heat = sum(2 ** (-(now - published).total_seconds() / (policy.heat_half_life_hours * 3600)) for published in latest.values())
    return len(latest), heat


def refresh_heat(now, policy):
    cutoff = stamp(now - timedelta(hours=policy.freshness_hours))
    groups = {}
    for row in db.query("SELECT i.event_id,i.publisher_key,i.published_at FROM feed_items i WHERE i.state='clustered' AND i.published_at>=?", (cutoff,)):
        groups.setdefault(row["event_id"], []).append(row)
    expired = db.query("SELECT id FROM events WHERE heat_score>0 AND NOT EXISTS (SELECT 1 FROM feed_items WHERE state='clustered' AND event_id=events.id AND published_at>=?)", (cutoff,))
    updates = [(*metrics_for(rows, now, policy), identifier) for identifier, rows in groups.items()] + [(0, 0, row["id"]) for row in expired]
    if updates:
        with db.transaction() as connection:
            connection.executemany("UPDATE events SET independent_source_count=?,heat_score=? WHERE id=?", updates)


def show_heat(events, now, policy):
    if not events:
        return
    event_ids = {event["id"] for event in events}
    groups = {identifier: [] for identifier in event_ids}
    cutoff = stamp(now - timedelta(hours=policy.freshness_hours))
    for row in db.query("SELECT event_id,publisher_key,published_at FROM feed_items WHERE state='clustered' AND published_at>=? AND event_id IN ("
                        + ",".join("?" for _ in event_ids) + ")", (cutoff, *event_ids)):
        groups[row["event_id"]].append(row)
    for event in events:
        event["independent_source_count"], event["heat_score"] = metrics_for(groups[event["id"]], now, policy)


def effective_score(item):
    return item["manual_score"] if item["manual_score"] is not None else item["score"]


def update_event(connection, event_id, now, policy, *, manual_assignment=False):
    event = connection.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
    items = connection.execute("SELECT * FROM feed_items WHERE event_id=? AND state='clustered' ORDER BY published_at,id", (event_id,)).fetchall()
    sync_event_links(connection, event_id, items, stamp(now))
    count, heat = metrics_for(items, now, policy)
    first_party = int(any(item["tier"] == "T1" for item in items))
    connection.execute("UPDATE events SET independent_source_count=?,heat_score=?,first_party_confirmed=?,updated_at=? WHERE id=?", (count, heat, first_party, stamp(now), event_id))
    if items and event["background"]:
        connection.execute("UPDATE events SET background=0 WHERE id=?", (event_id,))
    if event["cluster_locked"] and not manual_assignment:
        return
    if not items:
        connection.execute("UPDATE events SET representative_document_id=NULL WHERE id=?", (event_id,))
        return
    owners = {row[0] for row in connection.execute("SELECT entity_id FROM event_entities WHERE event_id=?", (event_id,))}

    def preference(item):
        rank = 2 if item["tier"] == "T1" else 1 if item["tier"] == "T1.5" and item["owner_entity_id"] in owners else 0
        return rank, effective_score(item) or 0

    top = max(items, key=preference)
    summary = station.decode(top["output_json"])
    first = min(row["published_at"] for row in items)
    if event["published_at"]:
        first = min(first, event["published_at"])
    connection.execute("UPDATE events SET representative_document_id=?,title=?,what_happened=?,why_important=?,event_type=?,product_version=?,published_at=? WHERE id=?",
                       (top["document_id"], summary["title"], summary["summary"], summary["importance_reason"], summary["event_type"], summary["version"], first, event_id))


def sync_event_links(connection, event_id, items, now):
    entity_ids = set()
    names = {}
    topics = {}
    for item in items:
        data = station.decode(item["output_json"])
        for entity in data["entities"]:
            names.setdefault((entity["kind"], station.normalized(entity["name"])), entity["name"])
        if item["tier"] == "T1" and item["owner_entity_id"]:
            entity_ids.add(item["owner_entity_id"])
        for topic_id in data["topic_ids"]:
            topics.setdefault(topic_id, data["importance_reason"])
    for (kind, key), name in names.items():
        connection.execute("INSERT OR IGNORE INTO entities(kind,name,normalized_name,state,created_at,updated_at) VALUES(?,?,?,'active',?,?)",
                           (kind, name, key, now, now))
        entity_ids.add(connection.execute("SELECT id FROM entities WHERE kind=? AND normalized_name=?", (kind, key)).fetchone()[0])
    entity_links = {row[0]: row[1] for row in connection.execute("SELECT entity_id,feed_only FROM event_entities WHERE event_id=?", (event_id,))}
    connection.executemany("DELETE FROM event_entities WHERE event_id=? AND entity_id=?",
                           ((event_id, identifier) for identifier, feed_only in entity_links.items() if feed_only and identifier not in entity_ids))
    connection.executemany("INSERT INTO event_entities(event_id,entity_id,feed_only) VALUES(?,?,1)",
                           ((event_id, identifier) for identifier in entity_ids - entity_links.keys()))
    topic_links = connection.execute("SELECT topic_id,impact FROM event_topics WHERE event_id=? AND feed_only=1", (event_id,)).fetchall()
    connection.executemany("DELETE FROM event_topics WHERE event_id=? AND topic_id=?",
                           ((event_id, row["topic_id"]) for row in topic_links if row["topic_id"] not in topics))
    connection.executemany("INSERT INTO event_topics(event_id,topic_id,impact,feed_only) "
                           "SELECT ?,id,?,1 FROM topics WHERE id=? AND research_state='active' "
                           "ON CONFLICT(event_id,topic_id) DO UPDATE SET impact=excluded.impact WHERE event_topics.feed_only=1 AND event_topics.impact!=excluded.impact",
                           ((event_id, impact, identifier) for identifier, impact in topics.items()))


def crosses_versions(version, *target_versions):
    version = station.normalized(version)
    known_versions = {station.normalized(value) for value in target_versions} - {""}
    return bool(version and known_versions - {version})


def commit(item, summary, decision, candidates, reviewed, now, policy):
    timestamp = stamp(now)
    with db.transaction() as connection:
        current = connection.execute("SELECT * FROM feed_items WHERE id=?", (item["id"],)).fetchone()
        if current["state"] != "selected" or current["event_id"] is not None:
            return current["event_id"]
        if not processing_source(connection, item["id"]):
            return None
        if effective_score(current) < policy.threshold(current["tier"], current["category"]):
            connection.execute("UPDATE feed_items SET state='scored_out',updated_at=? WHERE id=?", (timestamp, item["id"]))
            return None
        event_id = decision.event_id
        target = connection.execute("SELECT product_version FROM events WHERE id=? AND state='active' AND canonical_event_id IS NULL", (event_id,)).fetchone() if event_id else None
        if event_id and not target:
            event_id = None
            decision = Decision(relation="new_event", confidence="high", event_id=None, reason="原候选事件已经失效，已单独保留资料")
        if target and decision.relation == "same_event":
            candidate = next((row for row in candidates if row["id"] == event_id), {})
            if crosses_versions(summary.version, target["product_version"], candidate.get("product_version")):
                reason = "明确版本不同，按后续进展记录。" + decision.reason
                decision = decision.model_copy(update={"relation": "follow_up", "reason": reason[:500]})
        if not event_id:
            key = "feed:" + continuity.digest(current["canonical_url"])
            event_id = connection.execute(
                "INSERT INTO events(identity_key,event_type,title,what_happened,why_important,published_at,importance,created_at,updated_at,product_version) VALUES(?,?,?,?,?,?,?,?,?,?) RETURNING id",
                (key, summary.event_type, summary.title, summary.summary, summary.importance_reason, current["published_at"],
                 max(1, min(5, int(effective_score(current) / 20))), timestamp, timestamp, summary.version),
            ).fetchone()[0]
            connection.execute("INSERT INTO event_aliases(identity_key,event_id) VALUES(?,?)", (key, event_id))
        data = summary.model_dump()
        visible_at = db.now(timespec="microseconds")
        connection.execute("UPDATE feed_items SET event_id=?,state='clustered',output_json=?,error='',next_attempt_at=NULL,updated_at=?,event_visible_at=? WHERE id=?",
                           (event_id, station.encode(data), timestamp, visible_at, item["id"]))
        connection.execute("INSERT INTO event_cluster_decisions(item_id,candidates_json,similarity,relation,reviewed,reason,final_event_id,created_at) VALUES(?,?,?,?,?,?,?,?)",
                           (item["id"], station.encode(candidates), candidates[0]["similarity"] if candidates else None, decision.relation, int(reviewed), decision.reason, event_id, visible_at))
        update_event(connection, event_id, now, policy)
        return event_id


async def assign(item, summary, call, policy):
    now = datetime.now(timezone.utc)
    if item["cluster_candidates_json"] is None:
        candidates = await asyncio.to_thread(candidates_for, summary, now, policy)
        db.execute("UPDATE feed_items SET cluster_candidates_json=? WHERE id=?", (station.encode(candidates), item["id"]))
    else:
        candidates = station.decode(item["cluster_candidates_json"])
    similar = shortlist(candidates, policy)
    reviewed = False
    if similar:
        payload = {"item": summary.model_dump(), "published_at": item["published_at"], "candidates": similar}
        decision = await call("cluster", payload, Decision)
        if decision.confidence == "uncertain":
            reviewed = True
            decision = confirmed(await call("cluster_review", payload, Decision))
    else:
        decision = Decision(relation="new_event", confidence="high", event_id=None, reason="近期没有达到相似度阈值的事件")
    yield_to_manual()
    event_id = commit(item, summary, decision, candidates, reviewed, now, policy)
    if event_id:
        await asyncio.to_thread(knowledge.sync_event_vectors, now - timedelta(days=policy.cluster_days))
    return event_id


def correct_assignment(item_id, target_id, reason):
    now = datetime.now(timezone.utc)
    policy = selection()
    # 人工归属与 cluster_locked 在同一事务中写入。
    with db.transaction() as connection:
        item = connection.execute("SELECT * FROM feed_items WHERE id=? AND state='clustered'", (item_id,)).fetchone()
        target = connection.execute("SELECT * FROM events WHERE id=? AND state='active' AND canonical_event_id IS NULL", (target_id,)).fetchone()
        if not item or not target:
            raise ValueError("只能调整已聚簇条目，目标必须是有效事件")
        old_id = item["event_id"]
        version = station.decode(item["output_json"]).get("version")
        relation = "follow_up" if crosses_versions(version, target["product_version"]) else "same_event"
        visible_at = db.now(timespec="microseconds")
        connection.execute("UPDATE feed_items SET event_id=?,updated_at=?,event_visible_at=? WHERE id=?",
                           (target_id, stamp(now), visible_at if target_id != old_id else item["event_visible_at"], item_id))
        connection.execute("INSERT INTO event_cluster_decisions(item_id,relation,manual,reason,final_event_id,created_at) VALUES(?,?,1,?,?,?)", (item_id, relation, reason, target_id, visible_at))
        if relation == "follow_up":
            connection.execute("UPDATE events SET cluster_locked=1 WHERE id=?", (target_id,))
        for event_id in {old_id, target_id}:
            update_event(connection, event_id, now, policy, manual_assignment=event_id != target_id or relation == "same_event")
            connection.execute("UPDATE events SET cluster_locked=1 WHERE id=?", (event_id,))
        if old_id != target_id:
            merged = connection.execute("UPDATE events SET canonical_event_id=?,state='merged' WHERE id=? AND identity_key LIKE 'feed:%' AND NOT EXISTS(SELECT 1 FROM feed_items WHERE event_id=?) AND NOT EXISTS(SELECT 1 FROM event_claims WHERE event_id=?)", (target_id, old_id, old_id, old_id))
            if merged.rowcount:
                connection.execute("UPDATE event_aliases SET event_id=? WHERE event_id=?", (target_id, old_id))
