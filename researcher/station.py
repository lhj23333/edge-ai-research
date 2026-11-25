import hashlib
import json
import re
import unicodedata
from contextlib import nullcontext

from . import db
from .industry import selection
from .providers import ResearchError, source_url

ENTITY_KINDS = {"厂商", "产品", "应用场景", "技术路线", "软件生态"}
PURPOSES = {"行业竞争", "部署决策"}
LABELS = {"公开披露", "厂商宣称", "分析判断", "待核实"}


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def decode(value):
    return json.loads(value)


def normalized(value):
    return re.sub(r"\s+", " ", value or "").strip().casefold()


def status():
    return db.one("SELECT * FROM station_settings WHERE id=1")


def record_step(task_id, stage, status_value, inputs=None, outputs=None):
    timestamp = db.now()
    db.execute(
        "INSERT INTO task_steps(task_id,stage,status,inputs_json,outputs_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?) "
        "ON CONFLICT(task_id,stage) DO UPDATE SET status=excluded.status,inputs_json=excluded.inputs_json,outputs_json=excluded.outputs_json,updated_at=excluded.updated_at",
        (task_id, stage, status_value, encode(inputs or {}), encode(outputs or {}), timestamp, timestamp),
    )


def link_document(connection, document_id, task_id, version_id):
    connection.execute("INSERT INTO document_task_links(document_id,task_id,recorded_version_id) VALUES(?,?,?) "
                       "ON CONFLICT(document_id,task_id) DO UPDATE SET recorded_version_id=excluded.recorded_version_id",
                       (document_id, task_id, version_id))


def save_document(source, task_id):
    from .continuity import canonical_url
    original_url = source_url(source.url)
    url = canonical_url(original_url)
    content_hash = hashlib.sha256(source.content.encode()).hexdigest()
    timestamp = db.now()
    with db.transaction() as connection:
        previous = connection.execute("SELECT d.* FROM source_aliases a JOIN documents d ON d.id=a.document_id WHERE a.url IN (?,?) LIMIT 1", (url, original_url)).fetchone()
        if not previous:
            previous = connection.execute("SELECT * FROM documents WHERE url=?", (url,)).fetchone()
        if previous:
            url = previous["url"]
        preserve = previous and "原文" in previous["extent"] and "原文" not in source.extent
        connection.execute(
            "INSERT INTO documents(url,title,published_at,first_seen_at,last_fetched_at,extent,content,content_hash) VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(url) DO NOTHING",
            (url, source.title[:300], source.published_at, timestamp, timestamp, source.extent, source.content, content_hash),
        )
        document_id = connection.execute("SELECT id FROM documents WHERE url=?", (url,)).fetchone()[0]
        for alias in {original_url, canonical_url(original_url), url}:
            connection.execute("INSERT OR IGNORE INTO source_aliases(url,document_id) VALUES(?,?)", (alias, document_id))
        if previous and not preserve:
            connection.execute("UPDATE documents SET title=?,published_at=coalesce(?,published_at),last_fetched_at=?,extent=?,content=?,content_hash=? WHERE id=?", (source.title[:300], source.published_at, timestamp, source.extent, source.content, content_hash, document_id))
        connection.execute(
            "INSERT OR IGNORE INTO document_versions(document_id,content_hash,content,extent,fetched_at,verification) VALUES(?,?,?,?,?,?)",
            (document_id, content_hash, source.content, source.extent, timestamp, "已保存原文，未人工核验" if "原文" in source.extent else "未核对原网页"),
        )
        version_id = connection.execute("SELECT id FROM document_versions WHERE document_id=? AND content_hash=?", (document_id, content_hash)).fetchone()[0]
        if task_id is not None:
            link_document(connection, document_id, task_id, version_id)
        connection.execute("INSERT OR IGNORE INTO ingestion_queue(version_id,task_id,updated_at) VALUES(?,?,?)", (version_id, task_id, timestamp))
    if previous and not preserve and previous["extent"] == source.extent and previous["content_hash"] != content_hash:
        from .knowledge import invalidate
        invalidate(document_id, "同一来源的内容版本发生变化，需要重新检查相关知识")
        db.execute("UPDATE research_cache SET status='expired' WHERE kind='search_web' AND status='success' AND EXISTS (SELECT 1 FROM json_each(outputs_json,'$.sources') s JOIN source_aliases a ON a.url=json_extract(s.value,'$.url') WHERE a.document_id=?)", (document_id,))
    return document_id, version_id


def save_document_chunk(connection, version_id, position, start, page, heading, content):
    connection.execute(
        "INSERT OR IGNORE INTO document_chunks(version_id,position,start_offset,end_offset,page,heading,content,content_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
        (version_id, position, start, start + len(content), page, heading, content, hashlib.sha256(content.encode()).hexdigest(), db.now()),
    )


def save_claim(document_id, version_id, statement, excerpt, label, *, manual_excerpt=False):
    if label not in LABELS:
        raise ResearchError("证据类别无效")
    document = db.one("SELECT * FROM document_versions WHERE id=? AND document_id=?", (version_id, document_id))
    if not document:
        raise ResearchError("证据引用了不存在的资料版本")
    if not excerpt.strip() or normalized(excerpt) not in normalized(document["content"]):
        raise ResearchError("证据摘录与检索内容不吻合，已停止保存分析结果")
    if manual_excerpt and excerpt != document["content"]:
        raise ResearchError("人工录入的摘录必须与保存的资料版本一致")
    verification = "检索内容吻合，原网页未独立核验"
    if label == "分析判断":
        verification = "分析判断，须结合所引依据阅读"
    if manual_excerpt:
        verification = "用户提供摘录，原网页未独立核验"
    with db.transaction() as connection:
        connection.execute(
            "INSERT OR IGNORE INTO claims(document_id,document_version_id,statement,excerpt,label,verification,created_at) VALUES(?,?,?,?,?,?,?)",
            (document_id, version_id, statement.strip(), excerpt.strip(), label, verification, db.now()),
        )
        claim_id = connection.execute("SELECT id FROM claims WHERE document_version_id=? AND statement=? AND label=?", (version_id, statement.strip(), label)).fetchone()[0]
        if manual_excerpt:
            if not connection.execute("SELECT 1 FROM document_chunks WHERE version_id=? LIMIT 1", (version_id,)).fetchone():
                title = connection.execute("SELECT title FROM documents WHERE id=?", (document_id,)).fetchone()[0]
                # 保留人工摘录的原文位置和内容。
                save_document_chunk(connection, version_id, 0, 0, None, title, excerpt)
                connection.execute("UPDATE ingestion_queue SET status='indexed',detail=?,updated_at=? WHERE version_id=?",
                                   ("已建立 1 个人工摘录片段", db.now(), version_id))
            connection.execute("INSERT OR IGNORE INTO claim_evidence(claim_id,chunk_id,verdict,reason,created_at) "
                               "SELECT ?,id,'insufficient',?,? FROM document_chunks WHERE version_id=?",
                               (claim_id, "已关联用户提供的摘录，尚待核验原网页与摘录对说法的支持程度", db.now(), version_id))
        return claim_id


def product_model(name):
    value = normalized(unicodedata.normalize("NFKC", name))
    value = re.sub(r"[‐‑–—]", "-", value)
    value = re.sub(r"[\s-]+(?:dnpu|npu|soc|chip)$", "", value)
    match = re.fullmatch(r"([a-z]{2,24})(?:[ -]([a-z]{1,8}))?[ -]?(\d+[a-z0-9]*)", value)
    if not match:
        return None
    prefix, series, model = match.groups()
    return prefix + (series or ""), model


def canonical_entity_id(connection, identifier):
    row = connection.execute("SELECT coalesce(canonical_entity_id,id) FROM entities WHERE id=?", (identifier,)).fetchone()
    return row[0] if row else None


def product_owners(connection, item):
    owners = {row[0] for row in connection.execute(
        "SELECT r.from_entity_id FROM relations r JOIN entities e ON e.id=r.from_entity_id "
        "WHERE r.to_entity_id=? AND r.relation_type='提供产品' AND e.kind='厂商'", (item["id"],))}
    if item["parent_id"] is not None:
        owners.add(item["parent_id"])
    return owners


def entity(kind, name, state="active", parent_id=None, summary="", *, connection=None):
    if kind not in ENTITY_KINDS or not name.strip():
        raise ValueError("研究对象类别或名称无效")
    key = normalized(name)
    timestamp = db.now()
    with (db.transaction() if connection is None else nullcontext(connection)) as connection:
        if parent_id is not None:
            parent_id = canonical_entity_id(connection, parent_id)
        if kind == "产品":
            exact = connection.execute("SELECT id FROM entities WHERE kind='产品' AND normalized_name=?", (key,)).fetchone()
            alias = connection.execute("SELECT entity_id FROM entity_aliases WHERE normalized_name=?", (key,)).fetchone()
            if exact or alias:
                identifier = canonical_entity_id(connection, (exact or alias)[0])
                connection.execute("UPDATE entities SET updated_at=?,parent_id=coalesce(parent_id,?) WHERE id=?",
                                   (timestamp, parent_id, identifier))
                return identifier
            model = product_model(name)
            if model:
                candidates = [row for row in connection.execute("SELECT * FROM entities WHERE kind='产品' AND canonical_entity_id IS NULL AND state!='archived'")
                              if product_model(row["name"]) == model]
                if len(candidates) == 1:
                    candidate = candidates[0]
                    owners = product_owners(connection, candidate) | ({parent_id} if parent_id else set())
                    if len(owners) <= 1:
                        connection.execute("INSERT INTO entity_aliases(normalized_name,name,entity_id) VALUES(?,?,?)",
                                           (key, name.strip(), candidate["id"]))
                        connection.execute("UPDATE entities SET updated_at=?,parent_id=coalesce(parent_id,?) WHERE id=?",
                                           (timestamp, parent_id, candidate["id"]))
                        return candidate["id"]
        connection.execute(
            "INSERT INTO entities(kind,name,normalized_name,summary,parent_id,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(kind,normalized_name) DO UPDATE SET updated_at=excluded.updated_at",
            (kind, name.strip(), key, summary, parent_id, state, timestamp, timestamp),
        )
        return connection.execute("SELECT id FROM entities WHERE kind=? AND normalized_name=?", (kind, key)).fetchone()[0]


def merge_product(connection, duplicate_id, target_id):
    duplicate_id, target_id = (canonical_entity_id(connection, identifier) for identifier in (duplicate_id, target_id))
    if not duplicate_id or not target_id or duplicate_id == target_id:
        raise ValueError("请选择两条不同的产品记录")
    duplicate, target = (connection.execute("SELECT * FROM entities WHERE id=? AND kind='产品'", (identifier,)).fetchone()
                         for identifier in (duplicate_id, target_id))
    if not duplicate or not target or target["state"] == "archived":
        raise ValueError("归并目标必须是未归档的产品")
    owners = product_owners(connection, duplicate) | product_owners(connection, target)
    if len(owners) > 1:
        raise ValueError("两条记录的归属厂商不同或存疑")
    models = [product_model(row["name"]) for row in (duplicate, target)]
    if all(models) and models[0][0] == models[1][0] and models[0] != models[1]:
        raise ValueError("同一型号前缀下的产品代号不同，无法归并")

    connection.execute("""INSERT OR IGNORE INTO entity_claims(entity_id,claim_id)
                           SELECT ?,claim_id FROM (
                             SELECT claim_id FROM entity_claims WHERE entity_id=?
                             UNION SELECT claim_id FROM entity_fields WHERE entity_id=?
                             UNION SELECT claim_id FROM relations WHERE from_entity_id=? OR to_entity_id=?
                           ) WHERE claim_id IS NOT NULL""",
                       (target_id, duplicate_id, duplicate_id, duplicate_id, duplicate_id))
    connection.execute("""INSERT INTO entity_fields(entity_id,field_name,value,claim_id,updated_at)
                           SELECT ?,field_name,value,claim_id,updated_at FROM entity_fields WHERE entity_id=?
                           ON CONFLICT(entity_id,field_name,value) DO UPDATE
                           SET claim_id=coalesce(entity_fields.claim_id,excluded.claim_id)""", (target_id, duplicate_id))
    connection.execute("""WITH moved AS (
                             SELECT CASE WHEN from_entity_id=:old THEN :target ELSE from_entity_id END AS a,
                                    CASE WHEN to_entity_id=:old THEN :target ELSE to_entity_id END AS b,
                                    relation_type,explanation,claim_id,created_at
                             FROM relations WHERE from_entity_id=:old OR to_entity_id=:old
                           ) INSERT INTO relations(from_entity_id,to_entity_id,relation_type,explanation,claim_id,created_at)
                           SELECT a,b,relation_type,explanation,claim_id,created_at FROM moved WHERE a!=b
                           ON CONFLICT(from_entity_id,to_entity_id,relation_type) DO UPDATE
                           SET claim_id=coalesce(relations.claim_id,excluded.claim_id)""", {"old": duplicate_id, "target": target_id})
    connection.execute("""INSERT INTO event_entities(event_id,entity_id,feed_only)
                           SELECT event_id,?,feed_only FROM event_entities WHERE entity_id=?
                           ON CONFLICT(event_id,entity_id) DO UPDATE SET feed_only=min(feed_only,excluded.feed_only)""",
                       (target_id, duplicate_id))
    for table in ("entity_claims", "entity_fields", "event_entities"):
        connection.execute(f"DELETE FROM {table} WHERE entity_id=?", (duplicate_id,))
    connection.execute("DELETE FROM relations WHERE from_entity_id=? OR to_entity_id=?", (duplicate_id, duplicate_id))
    for table, column in (("entities", "parent_id"), ("feed_sources", "owner_entity_id"), ("feed_items", "owner_entity_id"),
                          ("knowledge_items", "entity_id"), ("knowledge_facets", "entity_id"), ("entity_aliases", "entity_id"),
                          ("entities", "canonical_entity_id")):
        connection.execute(f"UPDATE {table} SET {column}=? WHERE {column}=?", (target_id, duplicate_id))
    connection.execute("UPDATE annotations SET target_id=? WHERE target_kind='entity' AND target_id=?", (target_id, duplicate_id))
    connection.execute("INSERT INTO entity_aliases(normalized_name,name,entity_id) VALUES(?,?,?) "
                       "ON CONFLICT(normalized_name) DO UPDATE SET entity_id=excluded.entity_id", (duplicate["normalized_name"], duplicate["name"], target_id))
    connection.execute("UPDATE entities SET state='archived',followed=0,canonical_entity_id=?,updated_at=? WHERE id=?",
                       (target_id, db.now(), duplicate_id))
    connection.execute("""UPDATE entities SET followed=max(followed,?),parent_id=coalesce(?,nullif(parent_id,id)),
                           state=CASE WHEN ?='active' THEN 'active' ELSE state END,
                           summary=CASE WHEN summary='' THEN ? ELSE summary END,updated_at=? WHERE id=?""",
                       (duplicate["followed"], next(iter(owners), None), duplicate["state"], duplicate["summary"], db.now(), target_id))
    return target_id


def merge_existing_product_aliases(connection):
    groups = {}
    for item in connection.execute("SELECT * FROM entities WHERE kind='产品' AND state!='archived' ORDER BY length(normalized_name),id"):
        model = product_model(item["name"])
        if model:
            groups.setdefault(model, []).append(item)
    for items in groups.values():
        if len(items) > 1 and len(set().union(*(product_owners(connection, item) for item in items))) <= 1:
            for duplicate in items[1:]:
                merge_product(connection, duplicate["id"], items[0]["id"])


def add_event(item, claim_ids, entity_ids, topic_ids, existing_event_id=None):
    if item["event_type"] not in selection().event_types or not claim_ids:
        raise ResearchError("行业事件缺少有效类型或依据")
    for claim_id in claim_ids:
        if not db.one("SELECT id FROM claims WHERE id=?", (claim_id,)):
            raise ResearchError("行业事件引用了不存在的证据")
    key = normalized(item["identity_key"])
    if len(key) < 8:
        raise ResearchError("行业事件缺少可识别的厂商、进展或版本")
    evidence_dates = db.query(
        "SELECT d.published_at FROM claims c JOIN documents d ON d.id=c.document_id WHERE c.id IN (" + ",".join("?" for _ in claim_ids) + ") AND d.published_at IS NOT NULL",
        tuple(claim_ids),
    )
    publication = min((row["published_at"] for row in evidence_dates), default=None)
    with db.transaction() as connection:
        previous = connection.execute("SELECT e.id FROM event_aliases a JOIN events e ON e.id=a.event_id WHERE a.identity_key=?", (key,)).fetchone()
        if existing_event_id:
            target = connection.execute("SELECT id FROM events WHERE id=? AND state='active' AND canonical_event_id IS NULL", (existing_event_id,)).fetchone()
            if not target or previous and previous["id"] != target["id"]:
                raise ResearchError("事件编号和已有身份记录不一致")
            previous = target
        if previous:
            event_id = previous["id"]
            connection.execute("UPDATE events SET occurred_at=coalesce(occurred_at,?),"
                               "competitive_impact=CASE WHEN competitive_impact='' THEN ? ELSE competitive_impact END,updated_at=? WHERE id=?",
                               (item.get("occurred_at"), item.get("competitive_impact", ""), db.now(), event_id))
        else:
            cursor = connection.execute(
                "INSERT INTO events(identity_key,event_type,title,what_happened,why_important,competitive_impact,occurred_at,published_at,importance,background,created_at,updated_at,product_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (key, item["event_type"], item["title"], item["what_happened"], item["why_important"], item.get("competitive_impact", ""), item.get("occurred_at"), publication, item.get("importance", 1), int(item.get("background", False)), db.now(), db.now(), item.get("version", "")),
            )
            event_id = cursor.lastrowid
        connection.execute("INSERT OR IGNORE INTO event_aliases(identity_key,event_id) VALUES(?,?)", (key, event_id))
        for claim_id in claim_ids:
            connection.execute("INSERT OR IGNORE INTO event_claims(event_id,claim_id) VALUES(?,?)", (event_id, claim_id))
        for entity_id in entity_ids:
            entity_id = canonical_entity_id(connection, entity_id) or entity_id
            connection.execute("INSERT INTO event_entities(event_id,entity_id) VALUES(?,?) "
                               "ON CONFLICT(event_id,entity_id) DO UPDATE SET feed_only=0 WHERE event_entities.feed_only=1", (event_id, entity_id))
        for topic_id in topic_ids:
            connection.execute("INSERT INTO event_topics(event_id,topic_id,impact) VALUES(?,?,?) "
                               "ON CONFLICT(event_id,topic_id) DO UPDATE SET feed_only=0,impact=excluded.impact WHERE event_topics.feed_only=1",
                               (event_id, topic_id, item.get("why_important", "")))
    return event_id, previous is None


def revise(topic_id, purpose, conclusion, revision_status, boundaries, unresolved, reason, event_id, support_ids, oppose_ids):
    if purpose not in PURPOSES or revision_status not in {"初步判断", "得到加强", "需要修正", "存在争议", "待核实"}:
        raise ResearchError("专题判断类别无效")
    if not support_ids and not oppose_ids:
        raise ResearchError("专题判断没有引用证据")
    if not db.one("SELECT id FROM topics WHERE id=?", (topic_id,)):
        raise ResearchError("专题判断引用了不存在的专题")
    if event_id and not db.one("SELECT id FROM events WHERE id=?", (event_id,)):
        raise ResearchError("专题判断引用了不存在的事件")
    valid = db.query("SELECT id,label,verification FROM claims WHERE id IN (" + ",".join("?" for _ in support_ids + oppose_ids) + ")", tuple(support_ids + oppose_ids))
    if len(valid) != len(set(support_ids + oppose_ids)):
        raise ResearchError("专题判断引用了不存在的证据")
    if all(item["label"] in {"分析判断", "待核实"} for item in valid):
        raise ResearchError("专题判断缺少公开资料或厂商陈述作为依据")
    if any(item["id"] in support_ids and item["verification"] == "人工核对发现不符" for item in valid):
        raise ResearchError("已发现与原网页不符的说法不能支持当前判断")
    with db.transaction() as connection:
        connection.execute("INSERT OR IGNORE INTO judgments(topic_id,purpose) VALUES(?,?)", (topic_id, purpose))
        judgment = connection.execute("SELECT * FROM judgments WHERE topic_id=? AND purpose=?", (topic_id, purpose)).fetchone()
        previous = connection.execute("SELECT * FROM judgment_versions WHERE id=?", (judgment["current_version_id"],)).fetchone() if judgment["current_version_id"] else None
        if not previous and revision_status in {"得到加强", "需要修正"}:
            revision_status = "初步判断"
        if previous and normalized(previous["conclusion"]) == normalized(conclusion) and normalized(previous["boundaries"]) == normalized(boundaries) and normalized(previous["unresolved"]) == normalized(unresolved) and previous["status"] == revision_status and previous["event_id"] == event_id:
            old_support = {row[0] for row in connection.execute("SELECT claim_id FROM judgment_evidence WHERE version_id=? AND stance='支持'", (previous["id"],))}
            old_oppose = {row[0] for row in connection.execute("SELECT claim_id FROM judgment_evidence WHERE version_id=? AND stance='反对'", (previous["id"],))}
            if old_support == set(support_ids) and old_oppose == set(oppose_ids):
                return previous["id"], False
        cursor = connection.execute(
            "INSERT INTO judgment_versions(judgment_id,version,conclusion,status,boundaries,unresolved,reason,event_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (judgment["id"], previous["version"] + 1 if previous else 1, conclusion, revision_status, boundaries, unresolved, reason, event_id, db.now(timespec="microseconds")),
        )
        version_id = cursor.lastrowid
        for claim_id in support_ids:
            connection.execute("INSERT INTO judgment_evidence(version_id,claim_id,stance) VALUES(?,?,'支持')", (version_id, claim_id))
        for claim_id in oppose_ids:
            connection.execute("INSERT INTO judgment_evidence(version_id,claim_id,stance) VALUES(?,?,'反对')", (version_id, claim_id))
        connection.execute("UPDATE judgments SET current_version_id=?,status=? WHERE id=?", (version_id, revision_status, judgment["id"]))
    return version_id, True


def station_scope():
    companies = db.query("SELECT id,name,kind FROM entities WHERE followed=1 AND state='active' ORDER BY kind,name")
    topics = db.query("SELECT id,title,question FROM topics WHERE research_state='active' ORDER BY id")
    return {"entities": companies, "topics": topics}
