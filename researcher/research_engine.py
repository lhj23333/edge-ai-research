import asyncio
import hashlib
from datetime import datetime, timezone
from typing import TypedDict
from urllib.parse import urlsplit

import httpx
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.graph import END, START, StateGraph

from . import continuity, db, knowledge, research, station
from .config import DATA, ResearchSettings, settings
from .industry import selection
from .providers import Cancelled, Gateway, ResearchError, UnknownOutcome, yield_to_manual
from .research_models import Draft, DraftSubmission, LEGACY_PROMPT_VERSION, LegacyWebQuery, PLANNING, PROMPT_VERSION, SYNTHESIS, Review, TOOLS, tool_definitions


REQUEST_SNAPSHOT_VERSION = 2
INGEST_SETTINGS = {"ingest_requests", "automatic_event_research"}


class State(TypedDict, total=False):
    messages: list[dict]
    chunks: list[dict]
    memory: dict
    round: int
    calls: list[dict]
    visited: list[str]
    no_progress: int
    errors: list[str]
    finish: dict
    draft: dict
    review: dict
    outcome: dict


class ResearchRun:
    def __init__(self, task):
        self.task = task
        self.job = station.decode(task["job_json"])
        config = settings()
        self.config = ResearchSettings(**config["research"])
        self.legacy = "routes_fingerprint" not in self.job
        plan, review, write = research.check_routes(task, config)
        self.gateway = Gateway(plan.profile, task["id"], plan, legacy_research=self.legacy)
        self.reviewer = Gateway(review.profile, task["id"], review, legacy_research=self.legacy)
        self.writer = Gateway(write.profile, task["id"], write, legacy_research=self.legacy)
        self.prompt_version = self.job.get("prompt_version", LEGACY_PROMPT_VERSION)
        self.event_id = self.job.get("target_id") if self.job.get("target_kind") == "event" else self.job.get("conditions", {}).get("research_context", {}).get("event_id")
        if task["kind"] == "revise":
            self.event_id = self.job.get("trigger_event_id", self.event_id)
        self.private = bool(self.job.get("selected_note_ids"))
        self.can_read = not task.get("automatic") and not self.private and task["kind"] != "revise"
        self.can_search = (self.can_read and task["kind"] in {"interactive", "question", "event_research", "followup", "onboard"}
                           and plan.profile.provider in research.SEARCH_PROVIDERS and bool(plan.profile.search_verified))
        self.goal = self.job["main_question"]
        self.scope = self.job.get("scope", station.station_scope())
        self.started = datetime.fromisoformat(task["started_at"] or db.now())
        self.final_reserve = min(8, max(3, self.gateway.limits.task_requests - 3))

    def checkpoint(self, progress):
        if self.task.get("automatic"):
            yield_to_manual()
        self.gateway.checkpoint(progress)

    def record(self, stage, status="success", inputs=None, outputs=None):
        station.record_step(self.task["id"], stage, status, inputs, outputs)

    def request_identity(self):
        return {"version": REQUEST_SNAPSHOT_VERSION, "job_hash": continuity.digest(self.job),
                "config_hash": continuity.digest(self.config.model_dump(exclude=INGEST_SETTINGS)),
                "profile_fingerprint": self.task["profile_fingerprint"]}

    def matches_unversioned_config(self, expected_hash):
        config = self.config.model_dump(exclude=INGEST_SETTINGS)
        # 兼容包含采集配置的历史摘要。
        if self.legacy:
            return any(continuity.digest(dict(config, scan_requests=count)) == expected_hash for count in range(4, 101))
        return any(continuity.digest(dict(config, ingest_requests=count, automatic_event_research=enabled)) == expected_hash
                   for count in range(4, 101) for enabled in (False, True))

    def saved_request(self, key):
        previous = db.one("SELECT * FROM research_operations WHERE task_id=? AND operation_key=?", (self.task["id"], "request_inputs:" + key))
        if not previous:
            return None
        saved = station.decode(previous["inputs_json"])
        expected = self.request_identity()
        if "version" not in saved and self.matches_unversioned_config(saved.get("config_hash")):
            expected.pop("version")
            expected["config_hash"] = saved["config_hash"]
        if saved != expected:
            raise ResearchError("任务内容或研究配置已改变，请创建新任务；已保存的请求未重新发送")
        return station.decode(previous["outputs_json"])

    async def prepare_request(self, key, build):
        prepared = self.saved_request(key)
        if prepared is not None:
            return prepared
        prepared = await build()
        identity = self.request_identity()
        # 保存完整请求输入，供中断恢复复用。
        db.execute("INSERT INTO research_operations(task_id,operation_key,kind,status,inputs_json,outputs_json,created_at,updated_at) VALUES(?,?,'request_inputs','success',?,?,?,?)", (self.task["id"], "request_inputs:" + key, station.encode(identity), station.encode(prepared), db.now(), db.now()))
        return prepared

    def exhausted(self):
        return self.gateway.remaining() <= self.final_reserve or (datetime.now(timezone.utc) - self.started).total_seconds() >= self.config.timeout_minutes * 60

    def notes(self):
        notes = []
        for identifier in self.job.get("selected_note_ids", []):
            note = db.one("SELECT id,body,target_kind,target_id FROM annotations WHERE id=? AND archived=0", (identifier,))
            if not note:
                raise ResearchError("本次选择的个人批注已经不存在或归档")
            notes.append(note)
        return notes

    def refresh_required(self):
        if not self.can_read:
            return False
        if self.event_id:
            return bool(self.job.get("force_refresh"))
        return bool(self.job.get("force_refresh") or any(word in self.goal for word in ("最新", "今日", "近期", "最近")) or db.one("SELECT id FROM tasks WHERE prompt=? AND status='success' AND id!=? LIMIT 1", (self.goal, self.task["id"])))

    def base_context(self):
        target = None
        if self.task["kind"] == "revise":
            target = db.one("SELECT id,title,question FROM topics WHERE id=?", (self.job.get("target_id") or self.job.get("topic_id"),))
        elif self.event_id:
            target = db.one("SELECT id,title,event_type,what_happened,published_at,product_version,first_party_confirmed FROM events WHERE id=?", (self.event_id,))
        elif self.job.get("target_kind") == "topic":
            target = db.one("SELECT id,title,question FROM topics WHERE id=?", (self.job.get("target_id"),))
        elif self.job.get("target_kind") == "entity":
            target = db.one("SELECT id,name,kind,summary FROM entities WHERE id=?", (self.job.get("target_id"),))
        history = db.query("SELECT m.role,m.body FROM research_messages m JOIN tasks t ON t.id=m.task_id "
                           "WHERE m.session_id=? AND t.id<? AND coalesce(json_array_length(t.job_json,'$.selected_note_ids'),0)=0 "
                           "ORDER BY m.id DESC LIMIT 10", (self.task["session_id"], self.task["id"]))
        context = {"prompt_version": self.prompt_version, "utc_time": self.started.isoformat(), "task_kind": self.task["kind"], "goal": self.goal, "force_refresh": self.job.get("force_refresh", False), "refresh_required": self.refresh_required(), "completion": self.job.get("completion", "形成有原文依据的结论、适用条件与仍待核实的问题"), "conditions": self.job.get("conditions", {}), "subquestions": self.job.get("subquestions", []), "scope": self.scope, "target": target, "selected_personal_notes": self.notes(), "conversation": list(reversed(history))}
        if self.task.get("context_brief"):
            brief = db.one("SELECT title,body FROM briefs WHERE id=? AND topic_id=?", (self.task["context_brief"], self.task["topic_id"]))
            if brief:
                context["historical_brief"] = {"title": brief["title"], "body": brief["body"][:12000], "usage": "仅供理解历史讨论，关键结论须用原文核验。"}
        if self.task["kind"] == "revise":
            context["revision_purposes"] = sorted(station.PURPOSES)
            if self.event_id:
                context["trigger_event"] = db.one("SELECT id,title,event_type,what_happened,published_at,product_version,first_party_confirmed FROM events WHERE id=?", (self.event_id,))
        return context

    async def recall(self, state):
        self.checkpoint("检索已有领域知识与完整原文")
        chunks = await asyncio.to_thread(knowledge.search, self.goal)
        if self.event_id:
            seed = knowledge.event_chunks(self.event_id, max(4, self.config.context_chunks // 2))
            present = {item["chunk_id"] for item in seed}
            chunks = (seed + [item for item in chunks if item["chunk_id"] not in present])[:self.config.context_chunks]
        if self.job.get("chunk_ids"):
            chunks = [{**item, "chunk_id": item["id"]} for item in (knowledge.chunk_row(identifier) for identifier in self.job["chunk_ids"]) if item]
        memory = await asyncio.to_thread(knowledge.context, self.goal)
        self.record("研究目标与已有知识", inputs={"goal": self.goal, "scope": self.scope}, outputs={"chunk_ids": [item["chunk_id"] for item in chunks], "knowledge_ids": [item["id"] for item in memory["knowledge"]], "prompt_version": self.prompt_version})
        self.record("原文索引", outputs={"chunks": db.one("SELECT count(*) AS n FROM document_chunks")["n"]})
        context = self.base_context()
        context["existing_knowledge"] = memory
        context["initial_evidence"] = chunks
        if not self.can_read:
            context["mode"] = "仅整理本机已有证据，不使用联网工具；必须检查既有证据中的不同厂商与软件条件。"
        context["recent_searches"] = continuity.cache_candidates(self.task) if self.can_search else []
        return {"messages": [{"role": "system", "content": PLANNING}, {"role": "user", "content": station.encode(context)}], "chunks": chunks, "memory": memory, "round": 0, "calls": [], "visited": [], "no_progress": 0, "errors": [], "finish": {}}

    def tool_allowed(self, name):
        return {"search_web": self.can_search, "read_source": self.can_read}.get(name, True)

    def planner_tools(self, definitions):
        return [item for item in definitions if self.tool_allowed(item["function"]["name"])]

    async def plan(self, state):
        request_key = f"planner:{self.prompt_version}:{state['round']}"
        if self.saved_request(request_key) is None and (self.exhausted() or state["round"] >= self.config.max_rounds or state["no_progress"] >= 2):
            reason = "达到调查时间或请求额度限制" if self.exhausted() else "已达到轮次上限" if state["round"] >= self.config.max_rounds else "连续两轮未取得新的可用资料"
            self.record("调查停止", outputs={"reason": reason, "rounds": state["round"]})
            return {"finish": {"reason": reason, "complete": False}, "calls": []}
        self.checkpoint(f"根据证据规划第 {state['round'] + 1} 轮调查")
        async def build():
            messages = list(state["messages"])
            messages.append({"role": "user", "content": station.encode({"remaining_requests": self.gateway.remaining(), "reserved_final_requests": self.final_reserve, "round": state["round"] + 1, "unresolved_errors": state["errors"][-4:], "recent_searches": continuity.cache_candidates(self.task) if self.can_search else []})})
            return {"messages": messages, "tools": self.planner_tools(tool_definitions())}
        prepared = await self.prepare_request(request_key, build)
        messages = prepared["messages"]
        reply = await self.gateway.complete(messages, tools=self.planner_tools(prepared["tools"]), request_key=request_key, operation="plan")
        message = reply.message
        calls = message.get("tool_calls") or []
        if not calls:
            return {"messages": messages + [message], "calls": [], "finish": {"reason": reply.text or "模型结束调查", "complete": False}}
        if len(calls) > 2:
            raise ResearchError("模型单轮工具调用超过两个，已保留资料与返回内容")
        self.record(f"调查计划 {state['round'] + 1}", outputs={"actions": [{"tool": call["function"]["name"], "arguments": call["function"]["arguments"]} for call in calls]})
        return {"messages": messages + [message], "calls": calls}

    async def execute_tool(self, call, state):
        name = call["function"]["name"]
        if name not in TOOLS:
            raise ResearchError("模型请求了未提供的研究工具")
        if not self.tool_allowed(name):
            raise ResearchError("本次研究未启用联网搜索" if name == "search_web" else "本次研究限定为已归档资料")
        model = LegacyWebQuery if self.legacy and name == "search_web" else TOOLS[name][0]
        arguments = model.model_validate_json(call["function"]["arguments"])
        if name == "finish_research":
            if self.refresh_required() and not db.one("SELECT id FROM research_operations WHERE task_id=? AND kind IN ('search_web','read_source') AND status='success' LIMIT 1", (self.task["id"],)):
                return {"error": "本次要求检查最新资料，请先搜索公开网络或读取原始来源；当前不能声称已完成更新检查"}
            return {"finish": arguments.model_dump()}
        identity_arguments = arguments.model_dump()
        if name == "search_web":
            identity_arguments = {"query": continuity.normalized(arguments.query)}
            if continuity.measurement_units(arguments.query):
                identity_arguments["units"] = continuity.measurement_units(arguments.query)
        elif name == "read_source":
            identity_arguments = {"url": continuity.canonical_url(arguments.url)}
        signature = continuity.digest([name, identity_arguments])
        if signature in state["visited"]:
            return {"error": "该操作已经执行，当前资料未发生变化；请选择其他资料或结束调查", "signature": signature}
        key = "tool:" + call["id"]
        previous = db.one("SELECT * FROM research_operations WHERE task_id=? AND operation_key=?", (self.task["id"], key))
        if previous and previous["status"] == "success":
            return station.decode(previous["outputs_json"])
        self.checkpoint("读取研究资料：" + name)
        if name == "search_knowledge":
            chunks = await asyncio.to_thread(knowledge.search, arguments.query, None, arguments.include_history)
            result = {"chunks": chunks, "memory": await asyncio.to_thread(knowledge.context, arguments.query)}
        elif name == "search_web":
            sources = await self.gateway.search(arguments.query, request_key="search:" + signature, reuse_query_key=arguments.reuse_query_key, reserve_requests=self.final_reserve)
            documents = []
            for source in sources:
                previous_document = continuity.document_for_url(source.url)
                previous_version = db.one("SELECT id FROM document_versions WHERE document_id=? AND content_hash=?", (previous_document["id"], hashlib.sha256(source.content.encode()).hexdigest())) if previous_document else None
                identifier, version = station.save_document(source, self.task["id"])
                raw = station.encode({"title": source.title, "url": source.url, "content": source.content, "extent": source.extent, "published_at": source.published_at}).encode()
                db.execute("INSERT OR IGNORE INTO source_snapshots(version_id,content_type,content_hash,body,method,created_at) VALUES(?,'application/json',?,?,'provider_search',?)", (version, hashlib.sha256(raw).hexdigest(), raw, db.now()))
                documents.append({"document_id": identifier, "version_id": version, "url": source.url, "title": source.title, "extent": source.extent, "published_at": source.published_at, "change_type": "existing" if previous_version else "updated" if previous_document else "new"})
            await asyncio.to_thread(knowledge.ensure_index)
            chunks = await asyncio.to_thread(knowledge.search, arguments.query)
            new_ids = {item["document_id"] for item in documents}
            result = {"documents": documents, "chunks": [item for item in chunks if item["document_id"] in new_ids], "status": "checked" if sources else "no_results", **self.gateway.search_checks.get("search:" + signature, {})}
            query_key = hashlib.sha256(station.encode([arguments.query, self.scope]).encode()).hexdigest()
            db.execute("INSERT INTO research_experiences(query_key,query,scope_json,document_ids_json,last_result,last_used_at) VALUES(?,?,?,?,?,?) ON CONFLICT(query_key) DO UPDATE SET document_ids_json=excluded.document_ids_json,last_result=excluded.last_result,last_used_at=excluded.last_used_at", (query_key, arguments.query, station.encode(self.scope), station.encode(list(new_ids)), "检索取得资料" if sources else "本次查询未取得结果", db.now()))
        elif name == "read_source":
            result = await knowledge.fetch(arguments.url, self.task["id"], force=self.job.get("force_refresh", False))
            relevant = await asyncio.to_thread(knowledge.search, self.goal)
            selected = [item for item in relevant if item["document_id"] == result["document_id"]]
            result["chunks"] = selected or [{**item, **{key: value for key, value in knowledge.chunk_row(item["chunk_id"]).items() if key != "id"}} for item in result["chunks"][:4]]
        elif name == "read_evidence":
            chunks = [knowledge.chunk_row(identifier) for identifier in arguments.chunk_ids]
            if any(item is None for item in chunks):
                raise ResearchError("原文片段编号不存在")
            result = {"chunks": [{**item, "chunk_id": item["id"]} for item in chunks]}
        elif name == "inspect_topic":
            topic = db.one("SELECT id,title,question,research_state FROM topics WHERE id=?", (arguments.topic_id,))
            if not topic:
                raise ResearchError("专题不存在")
            result = {"topic": topic, "judgments": db.query("SELECT j.*,v.conclusion,v.boundaries,v.unresolved FROM judgments j JOIN judgment_versions v ON v.id=j.current_version_id WHERE j.topic_id=?", (arguments.topic_id,)), "events": db.query("SELECT e.* FROM events e JOIN event_topics et ON et.event_id=e.id WHERE et.topic_id=? AND e.state='active' ORDER BY e.id DESC", (arguments.topic_id,)), "chunks": db.query("SELECT DISTINCT c.id AS chunk_id,c.content,c.version_id,d.id AS document_id,d.title,d.url,v.fetched_at,v.extent,v.verification FROM judgments j JOIN judgment_evidence je ON je.version_id=j.current_version_id JOIN claim_evidence ce ON ce.claim_id=je.claim_id JOIN document_chunks c ON c.id=ce.chunk_id JOIN document_versions v ON v.id=c.version_id JOIN documents d ON d.id=v.document_id WHERE j.topic_id=? LIMIT 30", (arguments.topic_id,))}
        else:
            raise ResearchError("未知研究工具")
        result["signature"] = signature
        db.execute("INSERT INTO research_operations(task_id,operation_key,kind,status,inputs_json,outputs_json,created_at,updated_at) VALUES(?,?,?,'success',?,?,?,?) ON CONFLICT(task_id,operation_key) DO UPDATE SET status='success',outputs_json=excluded.outputs_json,updated_at=excluded.updated_at", (self.task["id"], key, name, station.encode(arguments.model_dump()), station.encode(result), db.now(), db.now()))
        return result

    async def act(self, state):
        results = await asyncio.gather(*(self.execute_tool(call, state) for call in state["calls"]), return_exceptions=True)
        chunks = {item["chunk_id"]: item for item in state["chunks"]}
        before_ids = set(chunks)
        before = {continuity.content_identity(item["content"]) for item in chunks.values()}
        messages, errors, visited = list(state["messages"]), list(state["errors"]), list(state["visited"])
        finish = {}
        for call, result in zip(state["calls"], results):
            if isinstance(result, (Cancelled, UnknownOutcome)):
                raise result
            if isinstance(result, BaseException):
                if not isinstance(result, (ResearchError, httpx.HTTPError, OSError, ValueError)):
                    raise result
                message = str(result) if isinstance(result, ResearchError) else f"工具执行失败（{type(result).__name__}）"
                result = {"error": message}
                errors.append(message)
                self.record(f"工具失败 {state['round'] + 1} {call['function']['name']}", "failed", outputs=result)
            for chunk in result.get("chunks", []):
                chunks[chunk["chunk_id"]] = chunk
            if result.get("signature"):
                visited.append(result["signature"])
            if result.get("finish"):
                finish = result["finish"]
            compact = dict(result)
            if "chunks" in compact:
                compact["chunks"] = compact["chunks"][:8]
            messages.append({"role": "tool", "tool_call_id": call["id"], "content": station.encode(compact)})
        round_number = state["round"] + 1
        self.record(f"调查执行 {round_number}", inputs={"tools": [item["function"]["name"] for item in state["calls"]]}, outputs={"new_chunk_ids": list(set(chunks) - before_ids), "errors": errors[-2:]})
        gained = {continuity.content_identity(item["content"]) for item in chunks.values()} - before
        return {"messages": messages, "chunks": list(chunks.values()), "round": round_number, "visited": visited, "no_progress": 0 if gained else state["no_progress"] + 1, "errors": errors, "finish": finish}

    async def synthesis_inputs(self, state):
        ranked = await asyncio.to_thread(knowledge.search, self.goal, self.config.context_chunks)
        candidates = {item["chunk_id"]: item for item in state["chunks"]}
        ordered = list(state["chunks"]) if self.job.get("chunk_ids") else [item for item in ranked if item["chunk_id"] in candidates]
        if self.event_id:
            seed = [item for item in knowledge.event_chunks(self.event_id, max(4, self.config.context_chunks // 2)) if item["chunk_id"] in candidates]
            selected_ids = {item["chunk_id"] for item in seed}
            ordered = seed + [item for item in ordered if item["chunk_id"] not in selected_ids]
        existing = {item["chunk_id"] for item in ordered}
        ordered.extend(item for item in reversed(state["chunks"]) if item["chunk_id"] not in existing)
        selected = ordered[:self.config.context_chunks]
        context = self.base_context()
        memory = await asyncio.to_thread(knowledge.context, self.goal)
        context.update(documents=selected, memory=memory, stop=state["finish"], errors=state["errors"], event_types=selection().event_types,
                       known_events=db.query("SELECT id,identity_key,title,event_type,product_version FROM events WHERE state='active' AND canonical_event_id IS NULL ORDER BY id=? DESC,id DESC LIMIT 100", (self.event_id,)))
        allowance = self.gateway.limits.task_requests
        context["output_limits"] = {"findings": 16 if allowance >= 24 else 8 if allowance >= 12 else 6, "points": 6 if allowance >= 24 else 4 if allowance >= 12 else 3, "optional_updates_total": 6 if allowance >= 24 else 2}
        if self.gateway.remaining() <= 8:
            context["output_limits"]["findings"] = min(context["output_limits"]["findings"], max(2, (self.gateway.remaining() - 4) * 2))
            context["output_limits"]["points"] = min(context["output_limits"]["points"], 2)
            context["output_limits"]["optional_updates_total"] = len(station.PURPOSES) if self.task["kind"] == "revise" else 1
        schema = DraftSubmission.model_json_schema()
        for field in ("findings", "points"):
            schema["$defs"]["Draft"]["properties"][field]["maxItems"] = context["output_limits"][field]
        context["schema"] = schema
        return {"messages": [{"role": "system", "content": SYNTHESIS}, {"role": "user", "content": station.encode(context)}], "chunks": selected, "memory": memory}

    async def synthesize(self, state):
        if state["finish"].get("needs_input"):
            draft = Draft(complete=False, completion_notes=state["finish"]["needs_input"])
            return {"draft": draft.model_dump()}
        request_key = "synthesis:json:" + self.prompt_version
        if self.saved_request(request_key) is None and self.gateway.remaining() < 2:
            draft = Draft(complete=False, completion_notes="剩余请求额度不足以完成结果整理和证据核验", uncertainties=["已取得的原文与调查记录已经保存，仍需完成整理和核验。"])
            return {"draft": draft.model_dump()}
        self.checkpoint("整理发现、证据与领域知识更新")
        prepared = await self.prepare_request(request_key, lambda: self.synthesis_inputs(state))
        reply = await self.writer.complete(prepared["messages"], structured=True, request_key=request_key, operation="synthesis")
        with self.writer.validate_result(request_key):
            draft = DraftSubmission.model_validate_json(reply.text).draft
            allowed = {item["chunk_id"] for item in prepared["chunks"]}
            refs = {item.ref for item in draft.findings}
            if len(refs) != len(draft.findings) or any(not set(item.chunk_ids) <= allowed for item in draft.findings):
                raise ResearchError("研究结果含重复说法编号或未提供的原文片段")
            if any(not set(item.finding_refs) <= refs for item in draft.points):
                raise ResearchError("回答引用了不存在的研究发现")
        self.record("研究结果草稿", "待核验", {"chunk_ids": list(allowed), "prompt_version": self.prompt_version}, {"draft": draft.model_dump()})
        return {"draft": draft.model_dump(), "chunks": prepared["chunks"], "memory": prepared["memory"]}

    async def review(self, state):
        from .research_review import review
        return await review(self, state)

    def publish(self, draft, review, reviews, chunks, accepted, rejected, accepted_gaps, update_errors):
        references, knowledge_ids, novel_refs = {}, [], set()
        for ref, finding in accepted.items():
            claim_ids = []
            for chunk_id in finding.chunk_ids:
                chunk = chunks[chunk_id]
                claim_id = station.save_claim(chunk["document_id"], chunk["version_id"], finding.statement, chunk["content"], finding.label)
                claim_ids.append(claim_id)
                db.execute("INSERT INTO claim_evidence(claim_id,chunk_id,task_id,verdict,reason,created_at) VALUES(?,?,?,'supported',?,?) ON CONFLICT(claim_id,chunk_id) DO UPDATE SET verdict=excluded.verdict,reason=excluded.reason,task_id=excluded.task_id", (claim_id, chunk_id, self.task["id"], reviews[ref].reason, db.now()))
                db.execute("INSERT OR IGNORE INTO document_task_links(document_id,task_id) VALUES(?,?)", (chunk["document_id"], self.task["id"]))
            references[ref] = list(dict.fromkeys(claim_ids))
            identifier = knowledge.put_knowledge(finding.statement, finding.kind, references[ref], self.task["id"], finding.conditions, self.job.get("target_id") if self.job.get("target_kind") == "topic" else self.job.get("topic_id"), self.job.get("target_id") if self.job.get("target_kind") == "entity" else None, reviews[ref].reason, existing_id=reviews[ref].existing_knowledge_id, change_type=reviews[ref].change_type, revalidated=reviews[ref].revalidated)
            knowledge_ids.append(identifier)
            if db.one("SELECT item_id FROM knowledge_changes WHERE task_id=? AND item_id=? AND change_type IN ('new','updated','conflict')", (self.task["id"], identifier)):
                novel_refs.add(ref)
        event_ids, revision_ids = [], []
        if self.event_id:
            versions = {item["version_id"] for item in knowledge.event_documents(self.event_id)}
            with db.transaction() as connection:
                for identifiers in references.values():
                    for identifier in identifiers:
                        if connection.execute("SELECT document_version_id FROM claims WHERE id=?", (identifier,)).fetchone()[0] in versions:
                            connection.execute("INSERT OR IGNORE INTO event_claims(event_id,claim_id) VALUES(?,?)", (self.event_id, identifier))
            event_ids.append(self.event_id)
        for index, event in enumerate(draft.events):
            if not event.existing_event_id and not set(event.claim_refs) & novel_refs and not db.one("SELECT event_id FROM event_aliases WHERE identity_key=?", (station.normalized(event.identity_key),)):
                continue
            if f"events:{index}" in rejected or not set(event.claim_refs) <= set(references):
                update_errors.append("行业事件未通过证据核验：" + event.title)
                continue
            try:
                cited = [chunks[identifier] for ref in event.claim_refs for identifier in accepted[ref].chunk_ids]
                claim_ids = list(dict.fromkeys(value for ref in event.claim_refs for value in references[ref]))
                event_id, _ = save_research_event(event, cited, claim_ids, self.scope)
                if event_id not in event_ids:
                    event_ids.append(event_id)
            except (ResearchError, ValueError) as error:
                update_errors.append("行业事件没有保存：" + str(error))
        for index, proposal in enumerate(draft.judgments):
            update_review = next((item for item in review.updates if item.category == "judgments" and item.index == index), None)
            if update_review and not update_review.has_change:
                continue
            refs = proposal.supporting_refs + proposal.opposing_refs
            if f"judgments:{index}" in rejected or not refs or not set(refs) <= set(references) or proposal.topic_id not in {item["id"] for item in self.scope.get("topics", [])}:
                update_errors.append("专题判断没有满足本次证据与作用范围要求")
                continue
            supports = list(dict.fromkeys(value for ref in proposal.supporting_refs for value in references[ref]))
            opposes = list(dict.fromkeys(value for ref in proposal.opposing_refs for value in references[ref]))
            revision_event_id = self.job.get("trigger_event_id") or (event_ids[0] if event_ids else None)
            identifier, created = station.revise(proposal.topic_id, proposal.purpose, proposal.conclusion, proposal.status, proposal.boundaries, proposal.unresolved, ("整理已有证据：" if self.task["kind"] == "revise" else "研究证据更新：") + proposal.reason, revision_event_id, supports, opposes)
            if created:
                revision_ids.append(identifier)
            knowledge_ids.append(knowledge.put_knowledge(proposal.conclusion, "专题判断", supports + opposes, self.task["id"], {"purpose": proposal.purpose, "boundaries": proposal.boundaries}, topic_id=proposal.topic_id, reason=proposal.reason))
        for index, proposal in enumerate(draft.fields):
            if f"fields:{index}" in rejected or proposal.finding_ref not in references:
                continue
            entity_id = station.entity(proposal.entity.kind, proposal.entity.name)
            db.execute("INSERT OR IGNORE INTO entity_fields(entity_id,field_name,value,claim_id,updated_at) VALUES(?,?,?,?,?)", (entity_id, proposal.field_name, proposal.value, references[proposal.finding_ref][0], db.now()))
        for index, proposal in enumerate(draft.relations):
            if f"relations:{index}" in rejected or proposal.finding_ref not in references:
                continue
            source = station.entity(proposal.source.kind, proposal.source.name)
            target = station.entity(proposal.target.kind, proposal.target.name)
            db.execute("INSERT OR IGNORE INTO relations(from_entity_id,to_entity_id,relation_type,explanation,claim_id,created_at) VALUES(?,?,?,?,?,?)", (source, target, proposal.relation_type, proposal.explanation, references[proposal.finding_ref][0], db.now()))
            if proposal.relation_type == "提供产品" and proposal.source.kind == "厂商" and proposal.target.kind == "产品":
                db.execute("UPDATE entities SET parent_id=? WHERE id=? AND parent_id IS NULL", (source, target))
        for index, proposal in enumerate(draft.proposals):
            if f"proposals:{index}" in rejected or proposal.finding_ref not in references:
                continue
            ref = accepted[proposal.finding_ref]
            document_id = chunks[ref.chunk_ids[0]]["document_id"]
            db.execute("INSERT INTO watchlist_proposals(kind,name,reason,document_id,created_at) VALUES(?,?,?,?,?) ON CONFLICT(kind,name) DO UPDATE SET reason=excluded.reason,document_id=excluded.document_id", (proposal.kind, proposal.name, (proposal.question + "\n" if proposal.question else "") + proposal.reason, document_id, db.now()))
        gaps = []
        for gap in accepted_gaps:
            claim_ids = list(dict.fromkeys(value for ref in gap.finding_refs if ref in references for value in references[ref]))
            gaps.append(research.save_gap(gap, self.task, claim_ids))
        with db.transaction() as connection:
            deferred_chunks = {identifier for finding in draft.findings if finding.ref in review.deferred_refs for identifier in finding.chunk_ids}
            reviewed_chunks = {identifier for finding in draft.findings if finding.ref not in review.deferred_refs and reviews[finding.ref].verdict != "insufficient" for identifier in finding.chunk_ids}
            for chunk_id in reviewed_chunks - deferred_chunks:
                if self.job.get("chunk_ids"):
                    connection.execute("UPDATE document_chunks SET processing_state='reviewed' WHERE id=?", (chunk_id,))
                connection.execute("INSERT INTO evidence_processing(chunk_id,context_key,task_id,status) VALUES(?,?,?,'reviewed') ON CONFLICT(chunk_id,context_key) DO UPDATE SET task_id=excluded.task_id,status=excluded.status", (chunk_id, continuity.processing_context(self.job), self.task["id"]))
            connection.execute("UPDATE ingestion_queue SET extraction_state='reviewed' WHERE version_id IN (SELECT version_id FROM document_chunks GROUP BY version_id HAVING sum(processing_state='pending')=0)")
        return references, knowledge_ids, event_ids, revision_ids, gaps

    async def commit(self, state):
        from .research_review import accepted_findings, verified_points

        self.checkpoint("保存私有研究结果" if self.private else "保存有效知识、判断修订和后续问题")
        draft = Draft.model_validate(state["draft"])
        review = Review.model_validate(state["review"])
        reviews = {item.ref: item for item in review.findings}
        chunks = {item["chunk_id"]: item for item in state["chunks"]}
        accepted = accepted_findings(draft, reviews)
        update_errors = []
        for ref, finding in list(accepted.items()):
            if db.one("SELECT id FROM claims WHERE statement=? AND verification='人工核对发现不符'", (finding.statement,)):
                del accepted[ref]
                update_errors.append("人工核验已指出该说法与原文不符，未再次采纳：" + finding.statement)
        rejected = set(review.rejected_updates)
        rejected.update(f"{item.category}:{item.index}" for item in review.updates if item.verdict != "supported")
        accepted_gaps = [gap for index, gap in enumerate(draft.gaps) if f"gaps:{index}" not in rejected]
        # 含个人批注的结果仅保存在私有研究中。
        if self.private:
            references = {ref: [] for ref in accepted}
            knowledge_ids, event_ids, revision_ids, gaps = [], [], [], []
        else:
            references, knowledge_ids, event_ids, revision_ids, gaps = self.publish(draft, review, reviews, chunks, accepted, rejected, accepted_gaps, update_errors)
        points = []
        point_reviews = {item.index: item for item in review.points}
        for point in verified_points(draft, reviews, point_reviews):
            if not set(point["finding_refs"]) <= set(references):
                continue
            cited = list(dict.fromkeys(value for ref in point["finding_refs"] for value in references[ref]))
            points.append({**point, "claim_ids": cited})
            if not self.private and point["category"] == "分析判断":
                knowledge_ids.append(knowledge.put_knowledge(point["text"], "研究分析", cited, self.task["id"], topic_id=self.job.get("topic_id"), reason="分析内容通过原文支持程度核验", existing_id=point_reviews[point["index"]].existing_knowledge_id, change_type="existing" if point_reviews[point["index"]].existing_knowledge_id else "new"))
        uncertainties = list(dict.fromkeys(review.uncertainties))
        freshness_checked = not self.refresh_required() or db.one("SELECT id FROM research_operations WHERE task_id=? AND kind IN ('search_web','read_source') AND status='success' LIMIT 1", (self.task["id"],))
        complete = bool(points) and draft.complete and review.completion_satisfied and bool(freshness_checked) and not state["errors"] and not state["finish"].get("needs_input")
        output = ["## 研究结论"]
        for point in points:
            citations = {}
            for ref in point["finding_refs"]:
                for chunk_id in accepted[ref].chunk_ids:
                    chunk = chunks[chunk_id]
                    document_id, version_id, url = chunk["document_id"], chunk["version_id"], chunk["url"]
                    saved_url = f"/documents/{document_id}?version={version_id}#chunk-{chunk_id}"
                    citations.setdefault((document_id, version_id), f"[资料 {document_id} · {urlsplit(url).hostname}](<{url}>)（[保存的原文]({saved_url})）")
            output.append(f"### {point['category']}\n\n{point['text']}\n\n" + " ".join(citations.values()))
        if not points:
            output.append("现有资料不足以形成通过证据核验的回答。已取得的资料和调查记录已经保存。")
        checked_completion = review.completion_notes or (draft.completion_notes if not draft.findings else "完成标准未全部满足，仍需检查保留的证据缺口。")
        stop_reason = "已有证据足以形成回答，已完成整理与核验。" if complete else "本次调查已结束，已保存有依据的内容与仍待核实的问题。"
        output.extend(["## 完成标准检查", checked_completion, "## 调查停止原因", stop_reason])
        if state["finish"].get("needs_input"):
            output.extend(["## 需要补充", state["finish"]["needs_input"]])
        if uncertainties:
            output.extend(["## 仍待核实", "\n".join("- " + value for value in uncertainties)])
        if accepted_gaps:
            output.extend(["## 后续研究", "\n".join("- " + item.question for item in accepted_gaps)])
        if update_errors:
            output.extend(["## 未采纳的知识更新", "\n".join("- " + value for value in update_errors)])
        body = "\n\n".join(output)
        used_chunk_ids = {identifier for finding in accepted.values() for identifier in finding.chunk_ids}
        result = {"answer": body, "summary": "\n".join(item["text"] for item in points[:3]), "points": points, "complete": complete, "outcome": "needs_input" if state["finish"].get("needs_input") else "complete" if complete else "partial", "needs_input": state["finish"].get("needs_input", ""), "uncertainties": uncertainties, "completion_notes": checked_completion, "stop_reason": state["finish"].get("reason"), "document_ids": list(dict.fromkeys(chunks[identifier]["document_id"] for identifier in used_chunk_ids)), "inspected_document_ids": list(dict.fromkeys(chunk["document_id"] for chunk in state["chunks"])), "chunk_ids": list(used_chunk_ids), "knowledge_ids": list(dict.fromkeys(knowledge_ids)), "gap_ids": gaps, "event_ids": event_ids, "revision_ids": revision_ids, "rounds": state["round"], "prompt_version": self.prompt_version}
        result["changes"] = [] if self.private else db.query("SELECT item_id,change_type,related_id,reason FROM knowledge_changes WHERE task_id=?", (self.task["id"],))
        checks = [station.decode(item["outputs_json"]).get("checked_at") for item in db.query("SELECT outputs_json FROM research_operations WHERE task_id=? AND kind IN ('search_web','read_source') AND status='success'", (self.task["id"],))]
        checked_times = [value for value in checks if value]
        if checked_times:
            result["checked_at"] = min(checked_times)
        if not self.private:
            self.record("知识保存", outputs={"knowledge_ids": result["knowledge_ids"], "gap_ids": gaps, "events": event_ids, "revisions": revision_ids})
        self.finish_result(result, body)
        return {"outcome": result}

    def finish_result(self, result, body):
        with db.transaction() as connection:
            if self.task["kind"] == "onboard" and not self.private:
                result["companies"] = connection.execute("SELECT count(*) FROM watchlist_proposals WHERE kind='company'").fetchone()[0]
                result["topics"] = connection.execute("SELECT count(*) FROM watchlist_proposals WHERE kind='topic'").fetchone()[0]
            connection.execute("UPDATE tasks SET status='success',result_json=?,result_text=?,progress=?,finished_at=? WHERE id=?", (station.encode(result), body, "研究完成" if result["complete"] else "已保存研究结果与待核实问题", db.now(), self.task["id"]))
            if not connection.execute("SELECT id FROM research_messages WHERE task_id=? AND role='assistant'", (self.task["id"],)).fetchone():
                connection.execute("INSERT INTO research_messages(session_id,task_id,role,body,created_at) VALUES(?,?,'assistant',?,?)", (self.task["session_id"], self.task["id"], body, db.now()))
            else:
                connection.execute("UPDATE research_messages SET body=? WHERE task_id=? AND role='assistant'", (body, self.task["id"]))
            connection.execute("UPDATE research_sessions SET status=?,updated_at=? WHERE id=?", (result["outcome"], db.now(), self.task["session_id"]))
            connection.execute("UPDATE research_questions SET status=?,updated_at=? WHERE task_id=?", ("answered" if result["complete"] else "partial", db.now(), self.task["id"]))
        if self.private:
            return
        research.finish_gap(self.task, result["complete"])
        used_documents = set(result["document_ids"])
        for experience in db.query("SELECT id,document_ids_json FROM research_experiences"):
            if set(station.decode(experience["document_ids_json"])) & used_documents:
                db.execute("UPDATE research_experiences SET useful_count=useful_count+?,failure_count=failure_count+? WHERE id=?", (int(bool(result["knowledge_ids"])), int(not result["complete"]), experience["id"]))

    def reusable_result(self):
        if self.task["kind"] not in {"interactive", "question"} or self.private or self.job.get("force_refresh") or any(word in self.goal for word in ("最新", "今日", "近期", "最近")):
            return None
        candidates = db.query("SELECT id,result_json,job_json,finished_at FROM tasks WHERE prompt=? AND context_brief IS ? AND status='success' AND id!=? ORDER BY id DESC LIMIT 10",
                              (self.goal, self.task.get("context_brief"), self.task["id"]))
        for candidate in candidates:
            job = station.decode(candidate["job_json"])
            result = station.decode(candidate["result_json"])
            from datetime import timedelta
            checked_at = result.get("checked_at") or candidate["finished_at"]
            if not checked_at or datetime.fromisoformat(checked_at) < datetime.now(timezone.utc) - timedelta(hours=self.config.query_cache_hours):
                continue
            if not result.get("complete") or result.get("prompt_version") != PROMPT_VERSION or not result.get("knowledge_ids") or job.get("conditions", {}) != self.job.get("conditions", {}) or job.get("target_kind") != self.job.get("target_kind") or job.get("target_id") != self.job.get("target_id"):
                continue
            if job.get("selected_note_ids") or job.get("scope") != self.scope or any(job.get(key) != self.job.get(key) for key in ("purpose", "completion", "subquestions")):
                continue
            items = [continuity.canonical_knowledge(identifier) for identifier in result["knowledge_ids"]]
            chunks = [knowledge.chunk_row(identifier) for identifier in result.get("chunk_ids", [])]
            if not chunks or any(item is None or item["status"] != "supported" for item in items) or any(item is None or not knowledge.fresh(item) for item in chunks):
                continue
            if any(knowledge.current_version(item["document_id"]) != item["version_id"] for item in chunks):
                continue
            result["knowledge_ids"] = list(dict.fromkeys(item["id"] for item in items))
            result["checked_at"] = checked_at
            result["changes"] = []
            result["answer"] = f"已有答案及证据仍在查询复用期限内，本次没有新增内容。\n\n[查看已有研究答案](/tasks/{candidate['id']})\n\n上次完成时间：{checked_at}。需要立即检查变化时请选择“检查最新资料”。"
            return {**result, "reused_task_id": candidate["id"]}
        return None


def save_research_event(event, chunks, claim_ids, scope):
    if event.event_type not in selection().event_types or not set(event.topic_ids) <= {item["id"] for item in scope["topics"]}:
        raise ResearchError("事件引用了未提供的类型或专题")
    if event.existing_event_id:
        existing = db.one("SELECT identity_key,event_type,product_version FROM events WHERE id=? AND state='active' AND canonical_event_id IS NULL", (event.existing_event_id,))
        if (not existing or station.normalized(event.identity_key) != existing["identity_key"] or event.event_type != existing["event_type"]
                or event.version and existing["product_version"] and event.version != existing["product_version"]):
            raise ResearchError("既有事件的身份、类别或版本与研究结果不一致")
    occurred = supported_event_date(event.occurred_at, chunks)
    entities = [station.entity(item.kind, item.name) for item in event.entities]
    return station.add_event({**event.model_dump(), "occurred_at": occurred, "background": True}, claim_ids, entities, event.topic_ids, event.existing_event_id)


def supported_event_date(value, chunks):
    if not value:
        return None
    day = datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    formats = {day.isoformat(), f"{day.year}/{day.month}/{day.day}", f"{day.year}.{day.month}.{day.day}",
               f"{day.year}年{day.month}月{day.day}日", f"{day.year}年{day.month:02d}月{day.day:02d}日",
               f"{day.strftime('%B')} {day.day}, {day.year}", f"{day.strftime('%b')} {day.day}, {day.year}"}
    if not any(date.casefold() in chunk["content"].casefold() for date in formats for chunk in chunks):
        raise ResearchError("事件发生日期缺少所引原文依据")
    return day.isoformat()


def route_plan(state):
    return "synthesize" if state.get("finish") or not state.get("calls") else "act"


def route_act(state):
    return "synthesize" if state.get("finish") else "plan"


async def perform(task):
    run = ResearchRun(task)
    reused = run.reusable_result()
    if reused:
        run.record("复用有效研究", outputs={"task_id": reused["reused_task_id"], "knowledge_ids": reused["knowledge_ids"]})
        run.finish_result(reused, reused["answer"])
        return
    builder = StateGraph(State)
    for name in ("recall", "plan", "act", "synthesize", "review", "commit"):
        builder.add_node(name, getattr(run, name))
    builder.add_edge(START, "recall")
    builder.add_edge("recall", "plan")
    builder.add_conditional_edges("plan", route_plan)
    builder.add_conditional_edges("act", route_act)
    builder.add_edge("synthesize", "review")
    builder.add_edge("review", "commit")
    builder.add_edge("commit", END)
    async with AsyncSqliteSaver.from_conn_string(str(DATA / "researcher.sqlite3")) as saver:
        graph = builder.compile(checkpointer=saver)
        config = {"configurable": {"thread_id": f"research:{task['id']}:{task['created_at']}"}, "recursion_limit": 100}
        snapshot = await graph.aget_state(config)
        if snapshot.values and run.job.get("context_policy_version") != research.CONTEXT_POLICY_VERSION:
            previous_private = db.one("SELECT id FROM tasks WHERE session_id=? AND id<? AND json_array_length(job_json,'$.selected_note_ids')>0 LIMIT 1", (task["session_id"], task["id"]))
            if run.private or previous_private:
                raise ResearchError("已有研究进度可能包含个人批注，请在会话中发起新一轮研究")
        await graph.ainvoke(None if snapshot.values else {}, config, durability="sync")
