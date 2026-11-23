import asyncio
import hashlib
import json
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from . import db


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


def normalized(value):
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def content_identity(value):
    return " ".join(unicodedata.normalize("NFC", value).split())


def measurement_units(value):
    return sorted(set(re.findall(r"(?<![A-Za-z])(?:[kKMGTPE]i?[bB](?:/s|ps)?|[pnumkKMGT]?W)(?![A-Za-z0-9])", value)))


def canonical_url(value):
    parts = urlsplit(value)
    hostname = (parts.hostname or "").lower()
    if parts.scheme not in {"http", "https"} or not hostname or parts.username or parts.password:
        raise ValueError("资料链接必须是不含凭据的 HTTP 或 HTTPS 地址")
    host = f"[{hostname}]" if ":" in hostname else hostname
    if parts.port and (parts.scheme, parts.port) not in {("http", 80), ("https", 443)}:
        host += ":" + str(parts.port)
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    cleaned = [(key, val) for key, val in pairs if not key.casefold().startswith("utm_") and key.casefold() not in {"fbclid", "gclid", "msclkid"}]
    query = parts.query if cleaned == pairs else urlencode(cleaned)
    return urlunsplit((parts.scheme.lower(), host, parts.path or "/", query, ""))


def document_for_url(url):
    return db.one("SELECT d.* FROM source_aliases a JOIN documents d ON d.id=a.document_id WHERE a.url=?", (canonical_url(url),)) or db.one("SELECT * FROM documents WHERE url=?", (url,))


def context_for_task(task):
    job = json.loads(task.get("job_json") or "{}")
    scope = job.get("scope", {})
    return {"scope": {"entities": sorted(item["id"] for item in scope.get("entities", [])), "topics": sorted(item["id"] for item in scope.get("topics", [])), "sources": scope.get("sources", {})}, "conditions": job.get("conditions", {}), "topic_id": task.get("topic_id"), "profile": task.get("profile_fingerprint")}


def processing_context(job):
    return digest({key: job.get(key) for key in ("main_question", "conditions", "target_kind", "target_id", "topic_id", "purpose", "completion", "subquestions")})


def cache_candidates(task, kind="search_web"):
    from .config import settings
    since = (datetime.now(timezone.utc) - timedelta(hours=settings()["research"]["query_cache_hours"])).isoformat()
    return db.query("SELECT cache_key,inputs_json,checked_at,task_id FROM research_cache WHERE kind=? AND context_key=? AND status='success' AND checked_at>=? ORDER BY checked_at DESC LIMIT 30", (kind, digest(context_for_task(task)), since))


def query_guards(query):
    return sorted(re.findall(r'(?:site|after|before):\S+|\S*\d\S*|"[^"]+"', normalized(query)))


async def cached_operation(kind, inputs, context, task_id, produce, force=False, reuse_key=None):
    from .config import settings
    from .providers import ResearchError, UnknownOutcome
    context_key = digest(context)
    cache_key = digest([kind, inputs, context_key])
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=settings()["research"]["query_cache_hours"])).isoformat()
    if reuse_key and not force:
        candidate = db.one("SELECT * FROM research_cache WHERE cache_key=? AND kind=? AND context_key=? AND status='success' AND checked_at>=?", (reuse_key, kind, context_key, cutoff))
        saved_inputs = json.loads(candidate["inputs_json"]) if candidate else {}
        if not candidate or query_guards(saved_inputs.get("query", "")) != query_guards(inputs.get("query", "")) or saved_inputs.get("units", []) != inputs.get("units", []):
            raise ResearchError("所选历史查询已失效或查询条件不同，请读取已有资料或执行新查询")
        return {**json.loads(candidate["outputs_json"]), "cache_key": reuse_key, "reused": True, "checked_at": candidate["checked_at"], "source_task_id": candidate["task_id"], "reused_query": json.loads(candidate["inputs_json"]).get("query")}
    for _ in range(6000):
        wait = False
        with db.transaction() as connection:
            row = connection.execute("SELECT * FROM research_cache WHERE cache_key=?", (cache_key,)).fetchone()
            if row and row["status"] == "success" and kind == "read_source":
                saved = json.loads(row["outputs_json"])
                head = connection.execute("SELECT v.id FROM documents d JOIN document_versions v ON v.document_id=d.id AND v.content_hash=d.content_hash WHERE d.id=?", (saved["document_id"],)).fetchone()
                if not head or head[0] != saved["version_id"]:
                    connection.execute("UPDATE research_cache SET status='expired' WHERE cache_key=?", (cache_key,))
                    row = None
            if row and row["status"] == "success" and (row["task_id"] == task_id or (not force and row["checked_at"] >= cutoff)):
                return {**json.loads(row["outputs_json"]), "cache_key": cache_key, "reused": True, "checked_at": row["checked_at"], "source_task_id": row["task_id"]}
            if row and row["status"] == "unknown":
                raise UnknownOutcome(f"相同研究请求的执行结果未知，请在原任务 #{row['task_id']} 页面选择继续处理")
            if row and row["status"] == "failed" and row["task_id"] == task_id:
                raise ResearchError("相同操作已经失败，请先检查原因，再在任务页面选择继续处理：" + json.loads(row["outputs_json"]).get("error", "未取得成功结果"))
            if row and row["status"] == "running":
                owner = connection.execute("SELECT status FROM tasks WHERE id=?", (row["task_id"],)).fetchone()
                if not owner or owner[0] not in {"queued", "running"}:
                    raise UnknownOutcome("相同研究请求曾中断，请在原任务页面选择继续处理")
                wait = True
            else:
                connection.execute("INSERT INTO research_cache(cache_key,kind,context_key,inputs_json,task_id,status,updated_at) VALUES(?,?,?,?,?,'running',?) ON CONFLICT(cache_key) DO UPDATE SET task_id=excluded.task_id,status='running',updated_at=excluded.updated_at", (cache_key, kind, context_key, encode(inputs), task_id, db.now()))
        if wait:
            await asyncio.sleep(0.1)
            continue
        try:
            result = await produce(cache_key)
        except BaseException as error:
            status = "unknown" if isinstance(error, (UnknownOutcome, asyncio.CancelledError)) else "failed"
            message = str(error) if isinstance(error, ResearchError) else "操作失败（" + type(error).__name__ + "）"
            db.execute("UPDATE research_cache SET status=?,outputs_json=?,updated_at=? WHERE cache_key=? AND task_id=?", (status, encode({"error": message}), db.now(), cache_key, task_id))
            raise
        checked_at = db.now()
        db.execute("UPDATE research_cache SET status='success',outputs_json=?,checked_at=?,updated_at=? WHERE cache_key=? AND task_id=?", (encode(result), checked_at, checked_at, cache_key, task_id))
        return {**result, "cache_key": cache_key, "reused": False, "checked_at": checked_at, "source_task_id": task_id}
    raise ResearchError("相同资料正在由其他操作获取，请等待该操作完成")


def canonical_knowledge(identifier):
    seen = set()
    while identifier:
        if identifier in seen:
            raise ValueError("知识合并记录存在循环")
        seen.add(identifier)
        row = db.one("SELECT * FROM knowledge_items WHERE id=?", (identifier,))
        if not row or not row["canonical_item_id"]:
            return row
        identifier = row["canonical_item_id"]
    return None


def fact_conditions(conditions):
    return {normalized(key): content_identity(str(value)) for key, value in conditions.items() if normalized(key) not in {"来源", "资料来源", "出处", "source", "sources"}}


def condition_values(value):
    numbers = {str(int(item)) if item.isdigit() else item for item in re.findall(r"\d+(?:\.\d+)*", value)}
    systems = {name for name in ("linux", "windows", "android", "macos", "freebsd") if name in value.casefold()}
    return numbers, systems, measurement_units(value)


def compatible_conditions(left, right, reviewed=False):
    left, right = fact_conditions(left), fact_conditions(right)
    # 表述不同的条件只有在模型明确核验为相同事实后才能合并；数值与系统差异仍阻止合并。
    return all(left[key] == right[key] or (reviewed and condition_values(left[key]) == condition_values(right[key])) for key in left.keys() & right.keys())


def item_compatible(identifier, conditions, entity_id=None, reviewed=False):
    facets = db.query("SELECT conditions_json,entity_id FROM knowledge_facets WHERE item_id=?", (identifier,))
    return all(compatible_conditions(json.loads(facet["conditions_json"]), conditions, reviewed) and (not entity_id or not facet["entity_id"] or entity_id == facet["entity_id"]) for facet in facets)


def record_change(connection, task_id, item_id, change_type, reason, related_id=None):
    if task_id is not None:
        connection.execute("INSERT OR IGNORE INTO knowledge_changes(task_id,item_id,change_type,related_id,reason) VALUES(?,?,?,?,?)", (task_id, item_id, change_type, related_id, reason))


def merge_knowledge(canonical_id, duplicate_id, reason):
    canonical, duplicate = canonical_knowledge(canonical_id), canonical_knowledge(duplicate_id)
    if not canonical or not duplicate or canonical["id"] == duplicate["id"]:
        return False
    canonical_id, duplicate_id = canonical["id"], duplicate["id"]
    with db.transaction() as connection:
        connection.execute("INSERT OR IGNORE INTO knowledge_evidence SELECT ?,claim_id FROM knowledge_evidence WHERE item_id=?", (canonical_id, duplicate_id))
        connection.execute("INSERT OR IGNORE INTO knowledge_facets SELECT ?,facet_key,kind,conditions_json,topic_id,entity_id FROM knowledge_facets WHERE item_id=?", (canonical_id, duplicate_id))
        # 历史纠错状态继续约束合并后的知识，不能因增加来源而恢复为已支持。
        status = "needs_review" if "needs_review" in {canonical["status"], duplicate["status"]} else canonical["status"]
        revision = canonical["revision"] + 1
        connection.execute("UPDATE knowledge_items SET status=?,revision=?,updated_at=? WHERE id=?", (status, revision, db.now(), canonical_id))
        connection.execute("INSERT INTO knowledge_versions(item_id,version,body,status,reason,created_at) VALUES(?,?,?,?,?,?)", (canonical_id, revision, canonical["body"], status, reason, db.now()))
        connection.execute("UPDATE knowledge_items SET canonical_item_id=?,status='merged' WHERE id=?", (canonical_id, duplicate_id))
    return True


def consolidate_history():
    from . import station
    counts = {"sources_imported": 0, "knowledge_merged": 0, "evidence_relinked": 0, "evidence_needs_review": 0}
    # 旧程序可能曾用摘要覆盖完整正文，优先恢复已经保存的完整原文作为当前版本。
    for document in db.query("SELECT id,extent FROM documents WHERE extent NOT LIKE '%原文%'"):
        version = db.one("SELECT * FROM document_versions WHERE document_id=? AND extent LIKE '%原文%' ORDER BY id DESC LIMIT 1", (document["id"],))
        if version:
            db.execute("UPDATE documents SET content=?,content_hash=?,extent=? WHERE id=?", (version["content"], version["content_hash"], version["extent"], document["id"]))
    from .providers import Source
    for source in db.query("SELECT * FROM sources ORDER BY id"):
        known = document_for_url(source["url"])
        station.save_document(Source(source["title"], source["url"], source["content"], source["extent"], source["published_at"]), source["task_id"], historical=True)
        counts["sources_imported"] += int(known is None)
    for document in db.query("SELECT id,url FROM documents ORDER BY id"):
        db.execute("INSERT OR IGNORE INTO source_aliases(url,document_id) VALUES(?,?)", (canonical_url(document["url"]), document["id"]))
    groups = {}
    for item in db.query("SELECT * FROM knowledge_items WHERE canonical_item_id IS NULL ORDER BY id"):
        key = content_identity(item["body"])
        candidates = groups.setdefault(key, [])
        previous = next((other for other in candidates if item_compatible(other["id"], json.loads(item["conditions_json"]), item["entity_id"])), None)
        if previous:
            counts["knowledge_merged"] += int(merge_knowledge(previous["id"], item["id"], "整理历史：相同事实保留来源、分类、条件和原有核验状态"))
        else:
            candidates.append(item)
    mismatches = db.query("SELECT e.*,c.statement,c.label,c.verification,c.document_version_id,ch.version_id,ch.content,v.document_id FROM claim_evidence e JOIN claims c ON c.id=e.claim_id JOIN document_chunks ch ON ch.id=e.chunk_id JOIN document_versions v ON v.id=ch.version_id WHERE c.document_version_id!=ch.version_id")
    for item in mismatches:
        claim_id = station.save_claim(item["document_id"], item["version_id"], item["statement"], item["content"], item["label"])
        with db.transaction() as connection:
            connection.execute("INSERT OR IGNORE INTO claim_evidence(claim_id,chunk_id,task_id,verdict,reason,created_at) VALUES(?,?,?,?,?,?)", (claim_id, item["chunk_id"], item["task_id"], "insufficient", "历史版本关联已纠正，原文支持程度需要重新核验", item["created_at"]))
            connection.execute("DELETE FROM claim_evidence WHERE claim_id=? AND chunk_id=?", (item["claim_id"], item["chunk_id"]))
            affected = connection.execute("SELECT k.* FROM knowledge_items k JOIN knowledge_evidence e ON e.item_id=k.id WHERE e.claim_id=? AND k.canonical_item_id IS NULL AND k.status!='needs_review'", (item["claim_id"],)).fetchall()
            for entry in affected:
                revision = entry["revision"] + 1
                connection.execute("UPDATE knowledge_items SET status='needs_review',revision=?,updated_at=? WHERE id=?", (revision, db.now(), entry["id"]))
                connection.execute("INSERT INTO knowledge_versions(item_id,version,body,status,reason,created_at) VALUES(?,?,?,'needs_review',?,?)", (entry["id"], revision, entry["body"], "历史证据的版本关联已纠正，需要重新核验原文支持程度", db.now()))
        counts["evidence_relinked"] += 1
        counts["evidence_needs_review"] += 1
    return counts
