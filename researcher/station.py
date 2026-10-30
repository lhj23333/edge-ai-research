import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from . import db
from .config import fingerprint, get_profile, settings
from .providers import ResearchError, source_url

ENTITY_KINDS = {"厂商", "产品", "应用场景", "技术路线", "软件生态"}
EVENT_TYPES = {"产品发布", "软件更新", "性能披露", "商业进展", "合作伙伴", "价格变化", "融资并购", "政策标准", "其他"}
PURPOSES = {"行业竞争", "部署决策"}
LABELS = {"公开披露", "厂商宣称", "分析判断", "待核实"}


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def decode(value):
    return json.loads(value)


def normalized(value):
    return re.sub(r"\s+", " ", value or "").strip().casefold()


def enqueue(kind, job=None, schedule_key=None):
    if kind not in {"onboard", "scan", "question", "revise"}:
        raise ValueError("研究任务类型无效")
    profile = get_profile()
    if not profile.chat_verified or (kind != "revise" and not profile.search_verified):
        raise ValueError("请在模型设置中完成连接和联网测试")
    if kind in {"onboard", "scan", "question"} and profile.provider not in {"deepseek", "zhipu", "moonshot"}:
        raise ValueError("当前资料检索需要可返回检索内容的服务商配置")
    if kind == "scan":
        station = status()
        if not station["watchlist_confirmed"] or not station["sources_confirmed"]:
            raise ValueError("请先确认关注名单和资料来源范围")
        if not db.one("SELECT id FROM entities WHERE followed=1 AND state='active' LIMIT 1") and not db.one("SELECT id FROM topics WHERE research_state='active' LIMIT 1"):
            raise ValueError("关注名单中没有启用的厂商或专题")
    job = job or {}
    if kind == "scan":
        job = {**job, "scope": station_scope()}
    with db.transaction() as connection:
        active = connection.execute("SELECT id FROM tasks WHERE kind IN ('onboard','scan','question','revise') AND status IN ('queued','running') LIMIT 1").fetchone()
        if active:
            raise ValueError("已有研究任务正在排队或执行，请等待完成")
        try:
            cursor = connection.execute(
                "INSERT INTO tasks(kind,status,prompt,profile_id,profile_fingerprint,provider,model,created_at,job_json,schedule_key) VALUES(?,'queued',?,?,?,?,?,?,?,?)",
                (kind, job.get("main_question", ""), profile.id, fingerprint(profile), profile.provider, profile.model, db.now(), encode(job), schedule_key),
            )
        except sqlite3.IntegrityError:
            if schedule_key:
                existing = connection.execute("SELECT id FROM tasks WHERE schedule_key=?", (schedule_key,)).fetchone()
                if existing:
                    return existing["id"]
            raise
        return cursor.lastrowid


def status():
    return db.one("SELECT * FROM station_settings WHERE id=1")


def retry(task_id):
    task = db.one("SELECT * FROM tasks WHERE id=?", (task_id,))
    if not task or task["kind"] not in {"onboard", "scan", "question", "revise"} or task["status"] not in {"failed", "interrupted", "cancelled"}:
        raise ValueError("该任务当前不能继续执行")
    if fingerprint(get_profile(task["profile_id"])) != task["profile_fingerprint"]:
        raise ValueError("任务使用的模型配置已变更，请重新发起研究")
    used = db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (task_id,))["n"]
    if used >= settings()["limits"]["task_requests"]:
        raise ValueError("该任务的调用额度已用尽，不能继续执行")
    with db.transaction() as connection:
        if connection.execute("SELECT id FROM tasks WHERE kind IN ('onboard','scan','question','revise') AND status IN ('queued','running') LIMIT 1").fetchone():
            raise ValueError("已有研究任务正在执行")
        connection.execute("UPDATE tasks SET status='queued',cancel_requested=0,error='',progress='等待继续处理已保存的资料',finished_at=NULL WHERE id=?", (task_id,))
        connection.execute("UPDATE research_questions SET status='running',updated_at=? WHERE task_id=?", (db.now(), task_id))
    return task_id


def record_step(task_id, stage, status_value, inputs=None, outputs=None):
    timestamp = db.now()
    db.execute(
        "INSERT INTO task_steps(task_id,stage,status,inputs_json,outputs_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?) "
        "ON CONFLICT(task_id,stage) DO UPDATE SET status=excluded.status,inputs_json=excluded.inputs_json,outputs_json=excluded.outputs_json,updated_at=excluded.updated_at",
        (task_id, stage, status_value, encode(inputs or {}), encode(outputs or {}), timestamp, timestamp),
    )


def save_document(source, task_id):
    url = source_url(source.url)
    content_hash = hashlib.sha256(source.content.encode()).hexdigest()
    timestamp = db.now()
    with db.transaction() as connection:
        connection.execute(
            "INSERT INTO documents(url,title,published_at,first_seen_at,last_fetched_at,extent,content,content_hash) VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(url) DO UPDATE SET title=excluded.title,published_at=coalesce(excluded.published_at,documents.published_at),last_fetched_at=excluded.last_fetched_at,extent=excluded.extent,content=excluded.content,content_hash=excluded.content_hash",
            (url, source.title[:300], source.published_at, timestamp, timestamp, source.extent, source.content, content_hash),
        )
        document_id = connection.execute("SELECT id FROM documents WHERE url=?", (url,)).fetchone()[0]
        if task_id is not None:
            connection.execute("INSERT OR IGNORE INTO document_task_links(document_id,task_id) VALUES(?,?)", (document_id, task_id))
        connection.execute(
            "INSERT OR IGNORE INTO document_versions(document_id,content_hash,content,extent,fetched_at,verification) VALUES(?,?,?,?,?,'服务商提取，未核对原网页')",
            (document_id, content_hash, source.content, source.extent, timestamp),
        )
        version_id = connection.execute("SELECT id FROM document_versions WHERE document_id=? AND content_hash=?", (document_id, content_hash)).fetchone()[0]
    return document_id, version_id


def save_claim(document_id, version_id, statement, excerpt, label):
    if label not in LABELS:
        raise ResearchError("证据类别无效")
    document = db.one("SELECT * FROM document_versions WHERE id=? AND document_id=?", (version_id, document_id))
    if not document:
        raise ResearchError("证据引用了不存在的资料版本")
    if not excerpt.strip() or normalized(excerpt) not in normalized(document["content"]):
        raise ResearchError("证据摘录与检索内容不吻合，已停止保存分析结果")
    verification = "检索内容吻合，原网页未独立核验"
    if label == "分析判断":
        verification = "分析判断，须结合所引依据阅读"
    with db.transaction() as connection:
        connection.execute(
            "INSERT OR IGNORE INTO claims(document_id,document_version_id,statement,excerpt,label,verification,created_at) VALUES(?,?,?,?,?,?,?)",
            (document_id, version_id, statement.strip(), excerpt.strip(), label, verification, db.now()),
        )
        return connection.execute("SELECT id FROM claims WHERE document_id=? AND statement=? AND label=?", (document_id, statement.strip(), label)).fetchone()[0]


def entity(kind, name, state="active", parent_id=None, summary=""):
    if kind not in ENTITY_KINDS or not name.strip():
        raise ValueError("研究对象类别或名称无效")
    key = normalized(name)
    timestamp = db.now()
    with db.transaction() as connection:
        connection.execute(
            "INSERT INTO entities(kind,name,normalized_name,summary,parent_id,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(kind,normalized_name) DO UPDATE SET updated_at=excluded.updated_at",
            (kind, name.strip(), key, summary, parent_id, state, timestamp, timestamp),
        )
        return connection.execute("SELECT id FROM entities WHERE kind=? AND normalized_name=?", (kind, key)).fetchone()[0]


def add_event(item, claim_ids, entity_ids, topic_ids):
    if item["event_type"] not in EVENT_TYPES or not claim_ids:
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
        previous = connection.execute("SELECT event_id AS id FROM event_aliases WHERE identity_key=?", (key,)).fetchone()
        if not previous and item["event_type"] == "产品发布" and entity_ids:
            event_date = (item.get("occurred_at") or publication or "")[:10]
            product_ids = [row[0] for row in connection.execute("SELECT id FROM entities WHERE kind='产品' AND id IN (" + ",".join("?" for _ in entity_ids) + ")", tuple(entity_ids))]
            if product_ids and event_date:
                previous = connection.execute(
                    "SELECT DISTINCT e.id FROM events e JOIN event_entities ee ON ee.event_id=e.id JOIN event_claims ec ON ec.event_id=e.id JOIN claims old_claim ON old_claim.id=ec.claim_id "
                    "WHERE e.state='active' AND e.event_type='产品发布' AND e.product_version=? AND abs(julianday(substr(coalesce(e.occurred_at,e.published_at),1,10))-julianday(?))<=1 "
                    "AND ee.entity_id IN (" + ",".join("?" for _ in product_ids) + ") AND old_claim.document_id IN (SELECT document_id FROM claims WHERE id IN (" + ",".join("?" for _ in claim_ids) + ")) ORDER BY e.id LIMIT 1",
                    (item.get("version", ""), event_date, *product_ids, *claim_ids),
                ).fetchone()
        if previous:
            event_id = previous["id"]
            connection.execute("UPDATE events SET updated_at=? WHERE id=?", (db.now(), event_id))
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
            connection.execute("INSERT OR IGNORE INTO event_entities(event_id,entity_id) VALUES(?,?)", (event_id, entity_id))
        for topic_id in topic_ids:
            connection.execute("INSERT OR IGNORE INTO event_topics(event_id,topic_id,impact) VALUES(?,?,?)", (event_id, topic_id, item.get("why_important", "")))
    return event_id, previous is None


def merge_events(canonical_id, duplicate_id):
    if canonical_id == duplicate_id:
        raise ValueError("不能将事件合并到自身")
    with db.transaction() as connection:
        canonical = connection.execute("SELECT * FROM events WHERE id=? AND state='active'", (canonical_id,)).fetchone()
        duplicate = connection.execute("SELECT * FROM events WHERE id=? AND state='active'", (duplicate_id,)).fetchone()
        if not canonical or not duplicate:
            raise ValueError("待合并的行业事件不存在或已经归档")
        connection.execute("INSERT OR IGNORE INTO event_claims(event_id,claim_id) SELECT ?,claim_id FROM event_claims WHERE event_id=?", (canonical_id, duplicate_id))
        connection.execute("INSERT OR IGNORE INTO event_entities(event_id,entity_id) SELECT ?,entity_id FROM event_entities WHERE event_id=?", (canonical_id, duplicate_id))
        connection.execute("INSERT OR IGNORE INTO event_topics(event_id,topic_id,impact) SELECT ?,topic_id,impact FROM event_topics WHERE event_id=?", (canonical_id, duplicate_id))
        connection.execute("UPDATE event_aliases SET event_id=? WHERE event_id=?", (canonical_id, duplicate_id))
        connection.execute("UPDATE events SET state='merged',canonical_event_id=?,updated_at=? WHERE id=?", (canonical_id, db.now(), duplicate_id))
        for issue in connection.execute("SELECT * FROM daily_issues").fetchall():
            ids = decode(issue["event_ids_json"])
            if duplicate_id not in ids:
                continue
            ids = [value for value in ids if value != duplicate_id]
            if canonical_id not in ids and canonical["created_at"] >= (issue["coverage_start"] or ""):
                ids.append(canonical_id)
            connection.execute("UPDATE daily_issues SET event_ids_json=? WHERE id=?", (encode(ids), issue["id"]))


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
        if previous and normalized(previous["conclusion"]) == normalized(conclusion) and previous["status"] == revision_status and previous["event_id"] == event_id:
            old_support = {row[0] for row in connection.execute("SELECT claim_id FROM judgment_evidence WHERE version_id=? AND stance='支持'", (previous["id"],))}
            old_oppose = {row[0] for row in connection.execute("SELECT claim_id FROM judgment_evidence WHERE version_id=? AND stance='反对'", (previous["id"],))}
            if old_support == set(support_ids) and old_oppose == set(oppose_ids):
                return previous["id"], False
        cursor = connection.execute(
            "INSERT INTO judgment_versions(judgment_id,version,conclusion,status,boundaries,unresolved,reason,event_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (judgment["id"], previous["version"] + 1 if previous else 1, conclusion, revision_status, boundaries, unresolved, reason, event_id, db.now()),
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
    return {"entities": companies, "topics": topics, "sources": decode(status()["coverage_json"])}


def coverage_window():
    current = datetime.now(timezone.utc)
    previous = status()["last_success_at"]
    return (previous or (current - timedelta(days=7)).isoformat(timespec="seconds"), current.isoformat(timespec="seconds"))


def start_run(task_id, scope):
    beginning, end = coverage_window()
    db.execute(
        "INSERT INTO collection_runs(task_id,window_start,window_end,scope_json,started_at) VALUES(?,?,?,?,?) "
        "ON CONFLICT(task_id) DO UPDATE SET status='running',details='',finished_at=NULL",
        (task_id, beginning, end, encode(scope), db.now()),
    )
    db.execute("UPDATE station_settings SET last_attempt_at=?,last_status='running',last_error='' WHERE id=1", (db.now(),))
    return db.one("SELECT * FROM collection_runs WHERE task_id=?", (task_id,))


def finish_run(run_id, run_status, event_ids, revision_ids, questions, details=""):
    if run_status not in {"complete", "partial", "failed"}:
        raise ValueError("采集结果状态无效")
    run = db.one("SELECT * FROM collection_runs WHERE id=?", (run_id,))
    with db.transaction() as connection:
        connection.execute("UPDATE collection_runs SET status=?,details=?,finished_at=? WHERE id=?", (run_status, details, db.now(), run_id))
        connection.execute("UPDATE station_settings SET last_status=?,last_error=? WHERE id=1", (run_status, details))
        if run_status in {"complete", "partial"}:
            if run_status == "complete":
                connection.execute("UPDATE station_settings SET last_success_at=? WHERE id=1", (db.now(),))
            connection.execute(
                "INSERT INTO daily_issues(run_id,coverage_start,coverage_end,status,event_ids_json,revision_ids_json,next_questions_json,created_at) VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(run_id) DO UPDATE SET status=excluded.status,event_ids_json=excluded.event_ids_json,revision_ids_json=excluded.revision_ids_json,next_questions_json=excluded.next_questions_json,created_at=excluded.created_at",
                (run_id, run["window_start"], run["window_end"], run_status, encode(event_ids), encode(revision_ids), encode(questions), db.now()),
            )


def local_day():
    return datetime.now(ZoneInfo(settings()["limits"]["timezone"])).date().isoformat()
