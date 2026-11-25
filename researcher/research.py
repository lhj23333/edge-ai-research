import hashlib
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone

from . import continuity, db, station
from .config import EffectiveRoute, ResearchSettings, fingerprint, get_profile, legacy_research_parameters, research_routes, routes_fingerprint, settings
from .industry import selection
from .research_models import PROMPT_VERSION
from .providers import ResearchError


TASK_KINDS = {"interactive", "onboard", "question", "revise", "followup", "event_research"}
TOOL_PROVIDERS = {"deepseek", "zhipu", "moonshot", "compatible"}
SEARCH_PROVIDERS = TOOL_PROVIDERS - {"compatible"}
CONTEXT_POLICY_VERSION = 1


def enqueue(kind, job=None, schedule_key=None, session_id=None, automatic=False, *, connection=None, brief_id=None):
    if kind not in TASK_KINDS:
        raise ValueError("研究任务类型无效")
    config = settings()
    plan, review, write = research_routes(config)
    for route in (plan, review, write):
        if not route.profile.chat_verified:
            raise ValueError("请先完成深度研究步骤所绑定模型的连接测试")
        if route.step != "deep_write" and route.profile.provider not in TOOL_PROVIDERS:
            raise ValueError("深度研究步骤需要支持原生工具调用的服务商")
    signature = fingerprint(plan.profile)
    job = dict(job or {})
    scope = job.get("scope") or station.station_scope()
    job["scope"] = scope
    if kind == "onboard":
        goal = selection().onboarding_goal
    elif kind == "revise":
        target = next((item for item in scope["topics"] if item["id"] == job.get("topic_id")), None)
        goal = job.get("main_question") or ("根据已有原文检查专题的行业竞争判断与部署决策，记录支持与反对证据、适用边界和仍待核实的问题。"
                                          + (f"《{target['title']}》的研究范围：{target['question']}" if target else ""))
    else:
        goal = job.get("main_question")
    if not goal:
        raise ValueError("请填写具体的研究目标")
    job.update(main_question=goal, prompt_version=PROMPT_VERSION, context_policy_version=CONTEXT_POLICY_VERSION,
               cost_profile=config["cost_profile"], routes_fingerprint=routes_fingerprint((plan, review, write)),
               routes=[{"step": route.step, "id": route.profile.id, "effort": route.reasoning_effort,
                        "tokens": route.output_tokens} for route in (plan, review, write)])
    limits = ResearchSettings(**config["research"])
    limit = limits.followup_requests if automatic and kind == "event_research" else limits.question_requests if kind in {"interactive", "event_research"} else getattr(limits, kind + "_requests")
    profile = plan.profile
    with (db.transaction() if connection is None else nullcontext(connection)) as connection:
        if kind == "event_research" and not session_id:
            existing = connection.execute("SELECT id FROM tasks WHERE kind='event_research' AND status IN ('queued','running') AND json_extract(job_json,'$.target_id')=? AND automatic=? LIMIT 1", (job["target_id"], int(automatic))).fetchone()
            if existing:
                return existing[0]
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
        cursor = connection.execute("INSERT INTO tasks(kind,status,prompt,profile_id,profile_fingerprint,provider,model,created_at,job_json,schedule_key,session_id,automatic,priority,request_limit,context_brief) VALUES(?,'queued',?,?,?,?,?,?,?,?,?,?,?,?,?)", (kind, goal, profile.id, signature, profile.provider, profile.model, db.now(), station.encode(job), schedule_key, session_id, int(automatic), 0 if automatic else 10, limit, brief_id))
        identifier = cursor.lastrowid
        if job.get("topic_id"):
            connection.execute("UPDATE tasks SET topic_id=? WHERE id=?", (job["topic_id"], identifier))
        connection.execute("INSERT INTO research_messages(session_id,task_id,role,body,created_at) VALUES(?,?,'user',?,?)", (session_id, identifier, goal, db.now()))
        connection.execute("UPDATE research_sessions SET status='running',updated_at=? WHERE id=?", (db.now(), session_id))
    return identifier


def topic_job(topic):
    target = {key: topic[key] for key in ("id", "title", "question")}
    return {"topic_id": target["id"], "target_kind": "topic", "target_id": target["id"],
            "scope": {"topics": [target], "entities": []}}


def enqueue_gap(gap, connection, *, automatic=False):
    if gap["status"] == "running":
        raise ValueError("该问题已经在研究中")
    job = {"main_question": gap["question"], "completion": gap["completion"], "target_kind": gap["target_kind"],
           "target_id": gap["target_id"], "gap_id": gap["id"]}
    if gap["target_kind"] == "topic":
        topic = connection.execute("SELECT id,title,question FROM topics WHERE id=?", (gap["target_id"],)).fetchone()
        if topic:
            job.update(topic_job(topic))
    elif gap["target_kind"] == "entity":
        entity = connection.execute("SELECT id,name,kind FROM entities WHERE id=?", (gap["target_id"],)).fetchone()
        if entity:
            job["scope"] = {"topics": [], "entities": [dict(entity)]}
    identifier = enqueue("followup", job, automatic=automatic, connection=connection,
                         schedule_key=f"gap:{gap['id']}:{gap['attempts']}" if automatic else None)
    connection.execute("UPDATE research_gaps SET task_id=?,status='running',attempts=attempts+1,updated_at=? WHERE id=?",
                       (identifier, db.now(), gap["id"]))
    return identifier


def retry(task_id):
    task = db.one("SELECT * FROM tasks WHERE id=?", (task_id,))
    if not task or not task["session_id"] or task["kind"] not in TASK_KINDS or task["status"] not in {"failed", "interrupted", "cancelled"}:
        raise ValueError("该任务当前不能继续处理")
    if db.one("SELECT task_id FROM topic_retry_quotas WHERE task_id=? AND (status='expired' OR auto_released_at<=?)", (task_id, db.now())):
        raise ValueError("本次定时研究已超时，请从专题页发起手动研究")
    check_routes(task)
    if db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (task_id,))["n"] >= (task["request_limit"] or settings()["limits"]["task_requests"]):
        raise ValueError("本任务的请求额度已经用尽")
    with db.transaction() as connection:
        active = connection.execute("SELECT id FROM tasks WHERE status IN ('queued','running') AND (session_id=? OR topic_id=?) LIMIT 1", (task["session_id"], task["topic_id"])).fetchone()
        if active:
            raise ValueError("该研究会话或专题已有任务")
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
        connection.execute("UPDATE topic_retry_quotas SET status='active' WHERE task_id=? AND status='finished'", (task_id,))
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
    with db.transaction() as connection:
        previous = connection.execute("SELECT evidence_json FROM research_gaps WHERE identity_key=?", (identity,)).fetchone()
        combined = list(dict.fromkeys((station.decode(previous[0]) if previous else []) + evidence_ids))
        connection.execute("INSERT INTO research_gaps(identity_key,question,reason,completion,priority,target_kind,target_id,origin_task_id,evidence_json,next_check_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(identity_key) DO UPDATE SET reason=excluded.reason,priority=max(priority,excluded.priority),evidence_json=excluded.evidence_json,updated_at=excluded.updated_at", (identity, gap.question, gap.reason, gap.completion, gap.priority, gap.target_kind, gap.target_id, task["id"], station.encode(combined), db.now(), db.now(), db.now()))
        return connection.execute("SELECT id FROM research_gaps WHERE identity_key=?", (identity,)).fetchone()[0]


def check_routes(task, config=None):
    job = station.decode(task["job_json"])
    config = config if config is not None else settings()
    if job.get("routes") and "routes_fingerprint" in job:
        routes = tuple(EffectiveRoute(row["step"], get_profile(row["id"], config), row["effort"], row["tokens"]) for row in job["routes"])
    else:
        parameters = legacy_research_parameters(config)
        legacy = "routes_fingerprint" not in job
        routes = []
        for step, prefix in (("deep_plan", "planning"), ("deep_review", "review"), ("deep_plan", "synthesis")):
            override = {} if legacy else config["model_routes"][step]
            profile = get_profile(override.get("profile_id") or (task["profile_id"] if legacy else None), config)
            routes.append(EffectiveRoute(step, profile, override.get("reasoning_effort", parameters[prefix + "_effort"]),
                                         override.get("output_tokens", parameters[prefix + "_output_tokens"])))
    if fingerprint(routes[0].profile) != task["profile_fingerprint"]:
        raise ValueError("研究规划使用的模型配置已经变更，请创建新任务")
    if "routes_fingerprint" in job and routes_fingerprint(routes) != job["routes_fingerprint"]:
        raise ValueError("研究步骤的模型、思考强度或输出上限已变更，请创建新任务")
    return routes


def event_schedule_key(connection, event_id):
    return f"event-research:{event_id}:{continuity.evidence_hash(connection, 'event', event_id)}"


def enqueue_event(event_id, *, automatic=False, schedule_key=None):
    event = db.one("SELECT id,title FROM events WHERE id=? AND state='active' AND canonical_event_id IS NULL", (event_id,))
    if not event:
        raise ValueError("事件不存在或已归档")
    scope = station.station_scope()
    scope["entities"] = db.query("SELECT x.id,x.name,x.kind FROM event_entities ee JOIN entities x ON x.id=ee.entity_id WHERE ee.event_id=? AND x.state!='archived'", (event_id,))
    related = db.query("SELECT t.id,t.title,t.question FROM event_topics et JOIN topics t ON t.id=et.topic_id WHERE et.event_id=? AND t.research_state='active'", (event_id,))
    if related:
        scope["topics"] = related
    job = {"target_kind": "event", "target_id": event_id, "scope": scope,
           "main_question": f"核验事件《{event['title']}》，说明已经确认的进展、适用条件和仍缺少的依据。",
           "completion": "把关键说法关联到一手原文及其他可用资料，区分厂商陈述、独立证据、分析判断和待核实信息。"}
    if automatic and schedule_key is None:
        with db.open_database() as connection:
            schedule_key = event_schedule_key(connection, event_id)
    return enqueue("event_research", job, schedule_key=schedule_key, automatic=automatic)


def finish_gap(task, complete):
    job = station.decode(task["job_json"])
    if job.get("gap_id"):
        next_check = datetime.now(timezone.utc) + timedelta(days=7 if not complete else 365)
        db.execute("UPDATE research_gaps SET status=?,next_check_at=?,updated_at=? WHERE id=? AND task_id=? AND status!='paused'",
                   ("resolved" if complete else "deferred", next_check.isoformat(timespec="seconds"), db.now(), job["gap_id"], task["id"]))


def feedback(session_id, task_id, kind, body="", item_id=None):
    if kind not in {"useful", "irrelevant", "incorrect", "outdated"}:
        raise ValueError("反馈类型无效")
    task = db.one("SELECT result_json,job_json FROM tasks WHERE id=? AND session_id=?", (task_id, session_id))
    if not task:
        raise ValueError("研究结果不存在")
    result = station.decode(task["result_json"])
    private = bool(station.decode(task["job_json"]).get("selected_note_ids"))
    identifiers = [] if private else list({item["id"] for item in (continuity.canonical_knowledge(identifier) for identifier in result.get("knowledge_ids", [])) if item})
    if item_id is not None:
        item = continuity.canonical_knowledge(item_id)
        if not item:
            raise ValueError("知识编号不存在")
        item_id = item["id"]
    if item_id is not None and item_id not in identifiers:
        raise ValueError("知识编号不属于该研究结果")
    db.execute("INSERT INTO research_feedback(session_id,task_id,item_id,kind,body,created_at) VALUES(?,?,?,?,?,?)", (session_id, task_id, item_id, kind, body, db.now()))
    if private:
        return
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
