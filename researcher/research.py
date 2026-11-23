import hashlib
from datetime import datetime, timedelta, timezone

from . import continuity, db, station
from .config import ResearchSettings, fingerprint, get_profile, settings
from .providers import ResearchError


def enqueue(kind, job=None, schedule_key=None, session_id=None, automatic=False, profile_id=None):
    if kind not in {"interactive", "onboard", "scan", "question", "revise", "followup"}:
        raise ValueError("研究任务类型无效")
    profile = get_profile(profile_id)
    if not profile.chat_verified or (kind != "revise" and not profile.search_verified):
        raise ValueError("请先完成当前模型的连接和联网测试")
    if profile.provider not in {"deepseek", "zhipu", "moonshot", "compatible"}:
        raise ValueError("当前模型适配器未验证原生研究工具调用")
    if kind in {"scan", "followup"}:
        state = station.status()
        if not state["watchlist_confirmed"] or not state["sources_confirmed"]:
            raise ValueError("请先确认关注名单与资料来源范围")
        if not station.station_scope()["entities"] and not station.station_scope()["topics"]:
            raise ValueError("关注范围中没有启用的对象")
    config = ResearchSettings(**settings()["research"])
    job = dict(job or {})
    scope = job.get("scope") or station.station_scope()
    job["scope"] = scope
    goal = job.get("main_question") or {"scan": "检查已确认关注范围自上次成功采集以来的变化，寻找有依据的进展、重要条件及需要修订的专题判断。", "onboard": "根据公开原始资料提出最多6家端侧与边缘推理独立 AI 加速器厂商及3个研究专题，并说明关注理由和证据。", "revise": "根据当前全部相关证据重新检查专题认识，分别维护行业竞争与部署决策判断，明确边界和证据缺口。"}.get(kind, "")
    if not goal:
        raise ValueError("请填写具体的研究目标")
    job["main_question"] = goal
    limit_name = "question_requests" if kind == "interactive" else kind + "_requests"
    limit = getattr(config, limit_name)
    with db.transaction() as connection:
        if schedule_key:
            old = connection.execute("SELECT id FROM tasks WHERE schedule_key=?", (schedule_key,)).fetchone()
            if old:
                return old[0]
        if job.get("topic_id") and connection.execute("SELECT id FROM tasks WHERE topic_id=? AND status IN ('queued','running')", (job["topic_id"],)).fetchone():
            raise ValueError("该专题已有研究正在执行，请等待完成或取消任务")
        if session_id:
            session = connection.execute("SELECT id FROM research_sessions WHERE id=?", (session_id,)).fetchone()
            if not session:
                raise ValueError("研究会话不存在")
            if connection.execute("SELECT id FROM tasks WHERE session_id=? AND status IN ('queued','running')", (session_id,)).fetchone():
                raise ValueError("该会话已有研究正在执行，请等待完成或取消任务")
        else:
            session_id = connection.execute("INSERT INTO research_sessions(title,purpose,scope_json,created_at,updated_at) VALUES(?,?,?,?,?)", (goal[:160], job.get("purpose", "行业竞争"), station.encode(scope), db.now(), db.now())).lastrowid
        cursor = connection.execute("INSERT INTO tasks(kind,status,prompt,profile_id,profile_fingerprint,provider,model,created_at,job_json,schedule_key,session_id,automatic,priority,request_limit) VALUES(?,'queued',?,?,?,?,?,?,?,?,?,?,?,?)", (kind, goal, profile.id, fingerprint(profile), profile.provider, profile.model, db.now(), station.encode(job), schedule_key, session_id, int(automatic), 0 if automatic else 10, limit))
        identifier = cursor.lastrowid
        if job.get("topic_id"):
            connection.execute("UPDATE tasks SET topic_id=? WHERE id=?", (job["topic_id"], identifier))
        connection.execute("INSERT INTO research_messages(session_id,task_id,role,body,created_at) VALUES(?,?,'user',?,?)", (session_id, identifier, goal, db.now()))
        connection.execute("UPDATE research_sessions SET status='running',updated_at=? WHERE id=?", (db.now(), session_id))
    return identifier


def retry(task_id):
    task = db.one("SELECT * FROM tasks WHERE id=?", (task_id,))
    if not task or task["status"] not in {"failed", "interrupted", "cancelled"}:
        raise ValueError("该任务当前不能继续处理")
    if fingerprint(get_profile(task["profile_id"])) != task["profile_fingerprint"]:
        raise ValueError("模型配置已发生变化，请创建新任务")
    if db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (task_id,))["n"] >= (task["request_limit"] or settings()["limits"]["task_requests"]):
        raise ValueError("本任务的请求额度已经用尽")
    with db.transaction() as connection:
        for operation in connection.execute("SELECT * FROM research_operations WHERE task_id=?", (task_id,)).fetchall():
            if any(marker in operation["operation_key"] for marker in (":truncated:", ":invalid:")):
                continue
            response = station.decode(operation["outputs_json"])
            truncated = bool(response.get("choices") and response["choices"][0].get("finish_reason") == "length")
            if truncated or operation["status"] in {"invalid", "truncated"}:
                status = "truncated" if truncated or operation["status"] == "truncated" else "invalid"
                connection.execute("INSERT OR IGNORE INTO research_operations(task_id,operation_key,kind,status,inputs_json,outputs_json,error,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)", (task_id, operation["operation_key"] + f":{status}:" + str(operation["id"]), operation["kind"], status, operation["inputs_json"], operation["outputs_json"], operation["error"], operation["created_at"], db.now()))
                connection.execute("UPDATE research_operations SET status='retry_authorized',updated_at=? WHERE id=?", (db.now(), operation["id"]))
        connection.execute("UPDATE research_operations SET status='retry_authorized',updated_at=? WHERE task_id=? AND status IN ('running','unknown','truncated') AND operation_key NOT LIKE '%:truncated:%'", (db.now(), task_id))
        connection.execute("UPDATE research_cache SET status='retry_authorized',updated_at=? WHERE task_id=? AND status IN ('running','unknown','failed')", (db.now(), task_id))
        connection.execute("UPDATE tasks SET status='queued',cancel_requested=0,error='',finished_at=NULL,progress='等待继续处理已保存资料' WHERE id=?", (task_id,))
        connection.execute("UPDATE research_sessions SET status='running',updated_at=? WHERE id=?", (db.now(), task["session_id"]))
    return task_id


def save_gap(gap, task, evidence_ids):
    if gap.existing_gap_id:
        existing = db.one("SELECT * FROM research_gaps WHERE id=? AND target_kind=? AND target_id IS ?", (gap.existing_gap_id, gap.target_kind, gap.target_id))
        if not existing:
            raise ResearchError("后续问题引用了不存在或对象不同的既有问题")
        combined = list(dict.fromkeys(station.decode(existing["evidence_json"]) + evidence_ids))
        db.execute("UPDATE research_gaps SET evidence_json=?,updated_at=? WHERE id=?", (station.encode(combined), db.now(), existing["id"]))
        return existing["id"]
    identity = hashlib.sha256(station.encode([station.normalized(gap.question), gap.target_kind, gap.target_id]).encode()).hexdigest()
    scope = station.decode(task["job_json"]).get("scope", {})
    allowed_topics = {item["id"] for item in scope.get("topics", [])}
    allowed_entities = {item["id"] for item in scope.get("entities", [])}
    scoped = (gap.target_kind == "topic" and gap.target_id in allowed_topics) or (gap.target_kind == "entity" and gap.target_id in allowed_entities)
    automatic = bool(evidence_ids) and (scoped or (gap.target_kind == "domain" and task["kind"] in {"scan", "followup"}))
    with db.transaction() as connection:
        previous = connection.execute("SELECT evidence_json FROM research_gaps WHERE identity_key=?", (identity,)).fetchone()
        combined = list(dict.fromkeys((station.decode(previous[0]) if previous else []) + evidence_ids))
        connection.execute("INSERT INTO research_gaps(identity_key,question,reason,completion,priority,target_kind,target_id,origin_task_id,evidence_json,next_check_at,automatic_allowed,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(identity_key) DO UPDATE SET reason=excluded.reason,priority=max(priority,excluded.priority),evidence_json=excluded.evidence_json,updated_at=excluded.updated_at", (identity, gap.question, gap.reason, gap.completion, gap.priority, gap.target_kind, gap.target_id, task["id"], station.encode(combined), db.now(), int(automatic), db.now(), db.now()))
        return connection.execute("SELECT id FROM research_gaps WHERE identity_key=?", (identity,)).fetchone()[0]


def schedule_followups():
    state = station.status()
    if not state["auto_enabled"] or not state["watchlist_confirmed"] or not state["sources_confirmed"]:
        return
    config = ResearchSettings(**settings()["research"])
    if db.one("SELECT id FROM tasks WHERE status IN ('queued','running') LIMIT 1"):
        return
    day = station.local_day()
    count = db.one("SELECT count(*) AS n FROM tasks WHERE kind IN ('followup','revise') AND automatic=1 AND substr(datetime(created_at,'+8 hours'),1,10)=?", (day,))["n"]
    used = db.one("SELECT count(*) AS n FROM usage u JOIN tasks t ON t.id=u.task_id WHERE u.day=? AND t.automatic=1", (day,))["n"]
    total = db.one("SELECT count(*) AS n FROM usage WHERE day=?", (day,))["n"]
    if count >= config.followups_per_day or used + config.followup_requests > config.automatic_daily_requests or total + config.followup_requests > settings()["limits"]["daily_requests"]:
        return
    gap = db.one("SELECT * FROM research_gaps WHERE status IN ('open','deferred') AND automatic_allowed=1 AND next_check_at<=? ORDER BY priority DESC,attempts,id LIMIT 1", (db.now(),))
    if not gap:
        pending = db.query("SELECT c.id FROM document_chunks c JOIN document_versions v ON v.id=c.version_id JOIN document_task_links l ON l.document_id=v.document_id JOIN tasks t ON t.id=l.task_id WHERE c.processing_state='pending' AND t.kind IN ('scan','followup') GROUP BY c.id ORDER BY c.id LIMIT ?", (config.context_chunks,))
        if pending:
            enqueue("revise", {"main_question": "整理本机已取得但尚未核验整理的原文片段，提取对确认关注领域有价值的事实、术语、条件和后续问题。只使用已有资料。", "chunk_ids": [row["id"] for row in pending]}, automatic=True)
        return
    if gap["target_kind"] == "topic" and not db.one("SELECT id FROM topics WHERE id=? AND research_state='active'", (gap["target_id"],)):
        return
    if gap["target_kind"] == "entity" and not db.one("SELECT id FROM entities WHERE id=? AND followed=1 AND state='active'", (gap["target_id"],)):
        return
    identifier = enqueue("followup", {"main_question": gap["question"], "completion": gap["completion"], "target_kind": gap["target_kind"], "target_id": gap["target_id"], "gap_id": gap["id"]}, schedule_key=f"gap:{gap['id']}:{gap['attempts']}", automatic=True)
    db.execute("UPDATE research_gaps SET task_id=?,status='running',attempts=attempts+1,updated_at=? WHERE id=?", (identifier, db.now(), gap["id"]))


def finish_gap(task, complete):
    job = station.decode(task["job_json"])
    if job.get("gap_id"):
        gap = db.one("SELECT status FROM research_gaps WHERE id=?", (job["gap_id"],))
        if gap and gap["status"] == "paused":
            return
        next_check = datetime.now(timezone.utc) + timedelta(days=7 if not complete else 365)
        db.execute("UPDATE research_gaps SET status=?,next_check_at=?,updated_at=? WHERE id=?", ("resolved" if complete else "deferred", next_check.isoformat(timespec="seconds"), db.now(), job["gap_id"]))


def feedback(session_id, task_id, kind, body="", item_id=None):
    if kind not in {"useful", "irrelevant", "incorrect", "outdated"}:
        raise ValueError("反馈类型无效")
    task = db.one("SELECT result_json FROM tasks WHERE id=? AND session_id=?", (task_id, session_id))
    if not task:
        raise ValueError("研究结果不存在")
    result = station.decode(task["result_json"])
    identifiers = list({item["id"] for item in (continuity.canonical_knowledge(identifier) for identifier in result.get("knowledge_ids", [])) if item})
    if item_id is not None:
        item = continuity.canonical_knowledge(item_id)
        if not item:
            raise ValueError("知识编号不存在")
        item_id = item["id"]
    if item_id is not None and item_id not in identifiers:
        raise ValueError("知识编号不属于该研究结果")
    db.execute("INSERT INTO research_feedback(session_id,task_id,item_id,kind,body,created_at) VALUES(?,?,?,?,?,?)", (session_id, task_id, item_id, kind, body, db.now()))
    documents = set(result.get("document_ids", []))
    for experience in db.query("SELECT id,document_ids_json FROM research_experiences"):
        if documents & set(station.decode(experience["document_ids_json"])):
            db.execute("UPDATE research_experiences SET useful_count=useful_count+?,failure_count=failure_count+?,last_result=? WHERE id=?", (int(kind == "useful"), int(kind != "useful"), {"useful": "用户反馈：相关研究有帮助", "irrelevant": "用户反馈：相关研究无关", "incorrect": "用户反馈：相关研究需要纠正", "outdated": "用户反馈：相关资料已经过时"}[kind], experience["id"]))
    if kind in {"incorrect", "outdated"}:
        from .knowledge import invalidate
        affected = [item_id] if item_id else identifiers
        for identifier in affected:
            for document in db.query("SELECT DISTINCT c.document_id FROM knowledge_evidence ke JOIN claims c ON c.id=ke.claim_id WHERE ke.item_id=?", (identifier,)):
                invalidate(document["document_id"], "用户指出研究结果需要重新核验：" + body)
        for gap_id in result.get("gap_ids", []):
            db.execute("UPDATE research_gaps SET status='open',next_check_at=?,updated_at=? WHERE id=? AND status!='paused'", (db.now(), db.now(), gap_id))
