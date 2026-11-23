import asyncio
import hashlib

from . import continuity, db, knowledge, station
from .config import mark_verified
from .providers import ResearchError
from .research_models import CompletionReview, Draft, EvidenceReview, FindingReview, PointReview, PROMPT_VERSION, REVIEW, Review, UpdateReview


def accepted_findings(draft, finding_reviews):
    result = {}
    for finding in draft.findings:
        review = finding_reviews[finding.ref]
        if finding.label not in {"公开披露", "厂商宣称"}:
            continue
        if review.verdict == "supported":
            result[finding.ref] = finding
        elif review.verdict == "partial" and review.supported_statement.strip():
            result[finding.ref] = finding.model_copy(update={"statement": review.supported_statement.strip()})
    return result


def verified_points(draft, finding_reviews, point_reviews):
    findings = {item.ref: item for item in draft.findings}
    accepted = accepted_findings(draft, finding_reviews)
    result = []
    for index, point in enumerate(draft.points):
        review = point_reviews[index]
        if review.verdict not in {"supported", "partial"}:
            continue
        refs = [ref for ref in point.finding_refs if ref in accepted]
        if not refs:
            continue
        if review.verdict == "supported" and len(refs) == len(point.finding_refs) and all(finding_reviews[ref].verdict == "supported" for ref in refs):
            result.append({**point.model_dump(), "index": index})
        else:
            # 部分成立的综合表达仅保留逐项核验通过的原始说法。
            result.append({"category": "适用边界" if point.category == "适用边界" else "来源陈述", "text": "\n\n".join(accepted[ref].statement for ref in refs), "finding_refs": refs, "index": index, "composed_from_verified_findings": True})
    return result


def references(category, item):
    if category == "events":
        return set(item.claim_refs)
    if category == "judgments":
        return set(item.supporting_refs + item.opposing_refs)
    if category == "gaps":
        return set(item.finding_refs)
    return {item.finding_ref}


async def prepare_payload(run, key, payload):
    async def build():
        return payload
    return await run.prepare_request(key, build)


async def submit(run, payload, model, name, operation):
    signature = hashlib.sha256(station.encode(payload).encode()).hexdigest()[:24]
    schema = model.model_json_schema()
    if model is EvidenceReview:
        schema["$defs"]["FindingReview"]["required"] = ["ref", "verdict", "reason", "existing_knowledge_id", "change_type", "dedup_reason", "revalidated", "supported_statement"]
        allowed_refs = [item["ref"] for item in payload["draft"]["findings"]]
        if allowed_refs:
            schema["$defs"]["FindingReview"]["properties"]["ref"]["enum"] = allowed_refs
        else:
            schema["properties"]["findings"]["maxItems"] = 0
        if payload["allowed_point_indices"]:
            schema["$defs"]["PointReview"]["properties"]["index"]["enum"] = payload["allowed_point_indices"]
        else:
            schema["properties"]["points"]["maxItems"] = 0
        if not payload["allowed_update_indices"]:
            schema["properties"]["updates"]["maxItems"] = 0
    reply = await run.gateway.complete([{"role": "system", "content": REVIEW}, {"role": "user", "content": station.encode(payload)}], tools=[{"type": "function", "function": {"name": name, "description": "提交当前核验步骤的结构化结果", "parameters": schema}}], reasoning_effort=run.config.review_effort, request_key=f"{operation}:{signature}", operation=operation, output_limit=run.config.review_output_tokens)
    with run.gateway.validate_result(f"{operation}:{signature}"):
        calls = reply.message.get("tool_calls") or []
        if len(calls) != 1 or calls[0]["function"]["name"] != name:
            raise ResearchError("模型没有提交当前步骤的结构化核验结果")
        result = model.model_validate_json(calls[0]["function"]["arguments"])
        if model is EvidenceReview and any(not {"existing_knowledge_id", "change_type", "dedup_reason", "revalidated"} <= item.model_fields_set for item in result.findings):
            raise ResearchError("原文核验缺少完整的事实去重判定，结果未保存为新知识")
    if not run.profile.tools_verified:
        mark_verified(run.profile, "tools", db.now(), run.task["profile_fingerprint"])
        run.profile.tools_verified = db.now()
    return result


async def review(run, state):
    draft = Draft.model_validate(state["draft"])
    if not draft.findings and not draft.points and not run.run:
        return {"review": Review(findings=[], points=[], completion_satisfied=False).model_dump()}
    run.checkpoint("逐条检查说法、条件和原文支持程度")
    findings = {item.ref: item for item in draft.findings}
    chunks = {item["chunk_id"]: item for item in state["chunks"]}
    checked_findings, checked_points, checked_updates, deferred = {}, {}, {}, []
    pending_updates = [(category, index, item) for category in ("events", "judgments", "fields", "relations", "proposals", "gaps") for index, item in enumerate(getattr(draft, category))]
    # 相同内容的核验通过请求摘要复用，不能仅凭 ref 或下标套用其他草稿的结果。

    async def unit(facts, points, updates):
        refs = {item.ref for item in facts}
        refs.update(ref for _, point in points for ref in point.finding_refs)
        refs.update(ref for category, _, item in updates for ref in references(category, item))
        if not refs <= set(findings):
            raise ResearchError("待核验内容引用了不存在的研究发现")
        source_ids = {identifier for ref in refs for identifier in findings[ref].chunk_ids}
        payload = {"prompt_version": PROMPT_VERSION, "goal": run.goal, "draft": {"findings": [item.model_dump() for item in facts], "points": [{**item.model_dump(), "index": index} for index, item in points]}, "context_findings": [findings[ref].model_dump() for ref in sorted(refs)], "evidence": [chunks[identifier] for identifier in sorted(source_ids)], "allowed_point_indices": [index for index, _ in points], "allowed_update_indices": [{"category": category, "index": index} for category, index, _ in updates], "instruction": "仅核验本批次给出的事实、回答要点和知识更新。points 和 updates 的 index 必须使用给定编号。其他批次不属于本次检查范围。只返回 findings、points、updates 三个数组，不包含整体完成标准的字段。调用 record_evidence_review 提交。"}
        payload["existing_knowledge"] = {ref: await asyncio.to_thread(knowledge.related_knowledge, findings[ref].statement, 8) for ref in sorted(refs)}
        for ref, candidates in payload["existing_knowledge"].items():
            for candidate in candidates:
                candidate["merge_compatible"] = continuity.item_compatible(candidate["id"], findings[ref].conditions, reviewed=True)
        payload["existing_point_knowledge"] = {str(index): await asyncio.to_thread(knowledge.related_knowledge, point.text, 5) for index, point in points}
        payload["current_judgments"] = state.get("memory", {}).get("judgments", [])
        payload["verified_findings"] = [item.model_dump() for item in checked_findings.values() if item.ref in refs]
        payload["instruction"] += "分析回答已有语义相同的研究分析时，在 PointReview.existing_knowledge_id 引用 existing_point_knowledge 中的编号。专题判断只是改写当前结论、边界及未解决问题时，UpdateReview.has_change=false；有依据的认识变化才填写 true。已有知识 needs_review 时检查 review_history 和 feedback，只有当前原文及条件确实解决原核验问题时 revalidated=true；仅增加来源或改写表述不能恢复有效状态。"
        payload["instruction"] += "候选的 merge_compatible 是服务器对适用条件的检查结果；false 时禁止将该候选判为 existing。需要修订或记录冲突时使用 updated 或 conflict，没有相同事实时 existing_knowledge_id=null 且 change_type=new。"
        payload["instruction"] += 'findings 只返回 draft.findings 的 ref，不重复返回 context_findings。原说法 partial 时，在 supported_statement 写出本批原文确实支持的完整表述，删除不成立或对象范围不同的部分；没有可保留的独立事实时填 JSON 空字符串 ""。其他 verdict 的 supported_statement 也填 ""。回答及更新依据 verified_findings 核验，不能重复使用已经被排除的部分。'
        for category in ("events", "judgments", "fields", "relations", "proposals", "gaps"):
            payload["draft"][category] = [{**item.model_dump(), "index": index} for kind, index, item in updates if kind == category]
        identity = continuity.digest({key: payload[key] for key in ("draft", "context_findings", "evidence", "verified_findings")})
        payload = await prepare_payload(run, f"evidence_review:{PROMPT_VERSION}:{identity}", payload)
        operation_key = "evidence_review:" + hashlib.sha256(station.encode(payload).encode()).hexdigest()[:24]
        cached = db.one("SELECT id FROM research_operations WHERE task_id=? AND operation_key=? AND status='success'", (run.task["id"], operation_key))
        if run.gateway.remaining() <= 1 and not cached:
            reason = "服务器额度检查：此项尚未完成核验，已保留原文和草稿"
            for item in facts:
                checked_findings[item.ref] = FindingReview(ref=item.ref, verdict="insufficient", reason=reason)
                deferred.append(item.ref)
            for index, item in points:
                checked_points[index] = PointReview(index=index, verdict="insufficient", reason=reason)
            for category, index, item in updates:
                checked_updates[(category, index)] = UpdateReview(category=category, index=index, verdict="insufficient", reason=reason)
            return
        result = await submit(run, payload, EvidenceReview, "record_evidence_review", "evidence_review")

        def reject(message):
            run.gateway.failed_operation(operation_key, "invalid", message)
            raise ResearchError(message)

        returned_refs = {item.ref for item in result.findings}
        if {item.ref for item in facts} != returned_refs or len(returned_refs) != len(result.findings):
            reject("说法核验编号或覆盖范围不完整")
        if {item.index for item in result.points} != {index for index, _ in points} or len(result.points) != len(points):
            reject("回答要点核验编号或覆盖范围不完整")
        if {(item.category, item.index) for item in result.updates} != {(category, index) for category, index, _ in updates} or len(result.updates) != len(updates):
            reject("知识更新缺少完整的逐项核验结果")
        for item in result.findings:
            if item.supported_statement and (item.verdict != "partial" or len(item.supported_statement.strip()) < 8):
                reject("部分支持的说法缺少有效的已核验表述")
            if item.existing_knowledge_id is not None:
                candidates = {candidate["id"]: candidate for candidate in payload["existing_knowledge"][item.ref]}
                candidate = candidates.get(item.existing_knowledge_id)
                if not candidate or item.change_type == "new":
                    reject("事实去重引用了候选范围之外的知识或缺少变化类型")
                if item.change_type == "existing" and not continuity.item_compatible(candidate["id"], findings[item.ref].conditions, reviewed=True):
                    reject("事实去重的适用条件不同，不能合并为已有知识：已有条件 " + candidate["conditions_json"] + "；本次条件 " + station.encode(findings[item.ref].conditions))
                if item.revalidated and any(knowledge.current_version(chunks[identifier]["document_id"]) != chunks[identifier]["version_id"] for identifier in findings[item.ref].chunk_ids):
                    reject("重新核验引用了已经被替换的资料版本")
            elif item.change_type != "new":
                reject("事实变化缺少对应的历史知识编号")
            prior = checked_findings.get(item.ref)
            if prior and prior.verdict != item.verdict:
                checked_findings[item.ref] = FindingReview(ref=item.ref, verdict="partial", reason="不同核验步骤对该说法的支持程度存在差异：" + prior.reason[:200] + "；" + item.reason[:200])
            else:
                checked_findings[item.ref] = item
        for item in result.points:
            if item.existing_knowledge_id is not None and item.existing_knowledge_id not in {candidate["id"] for candidate in payload["existing_point_knowledge"].get(str(item.index), [])}:
                reject("回答去重引用了候选范围之外的知识")
        checked_points.update((item.index, item) for item in result.points)
        checked_updates.update(((item.category, item.index), item) for item in result.updates)
        run.record("原文核验 " + hashlib.sha256(station.encode(payload).encode()).hexdigest()[:12], outputs=result.model_dump())

    for finding in draft.findings:
        if finding.ref in checked_findings:
            continue
        evidence = [db.one("SELECT e.reason,c.id FROM claim_evidence e JOIN claims c ON c.id=e.claim_id WHERE e.chunk_id=? AND c.document_version_id=? AND c.statement=? AND c.label=? AND e.verdict='supported' AND c.verification!='人工核对发现不符'", (identifier, chunks[identifier]["version_id"], finding.statement, finding.label)) for identifier in finding.chunk_ids]
        existing = next((item for item in state.get("memory", {}).get("knowledge", []) if item["status"] == "supported" and continuity.content_identity(item["body"]) == continuity.content_identity(finding.statement) and continuity.compatible_conditions(station.decode(item["conditions_json"]), finding.conditions)), None)
        if evidence and all(evidence) and existing and all(knowledge.current_version(chunks[identifier]["document_id"]) == chunks[identifier]["version_id"] for identifier in finding.chunk_ids):
            checked_findings[finding.ref] = FindingReview(ref=finding.ref, verdict="supported", reason=evidence[0]["reason"], existing_knowledge_id=existing["id"], change_type="existing", dedup_reason="相同事实、条件和原文版本已完成核验，复用既有核验结果")
    pending = [item for item in draft.findings if item.ref not in checked_findings]
    while pending:
        batch = pending[:4]
        source_ids = {identifier for item in batch for identifier in item.chunk_ids}
        if sum(len(chunks[identifier]["content"]) for identifier in source_ids) > 8000:
            batch = pending[:2]
        refs = {item.ref for item in batch}
        attached = [item for item in pending_updates if references(item[0], item[2]) and references(item[0], item[2]) <= refs][:4]
        await unit(batch, [], attached)
        pending = pending[len(batch):]
        pending_updates = [item for item in pending_updates if item not in attached]
    pending_points = [(index, item) for index, item in enumerate(draft.points) if index not in checked_points]
    while pending_points:
        batch = pending_points[:2]
        refs = {ref for _, point in batch for ref in point.finding_refs}
        source_ids = {identifier for ref in refs for identifier in findings[ref].chunk_ids}
        if sum(len(chunks[identifier]["content"]) for identifier in source_ids) > 12000:
            batch = pending_points[:1]
        await unit([], batch, [])
        pending_points = pending_points[len(batch):]
    while pending_updates:
        batch = pending_updates[:2]
        await unit([], [], batch)
        pending_updates = pending_updates[len(batch):]
    supported = verified_points(draft, checked_findings, checked_points)
    payload = {"prompt_version": PROMPT_VERSION, "goal": run.goal, "completion": run.job.get("completion", "形成有来源的结论、适用条件和仍待核实的问题"), "conditions": run.job.get("conditions", {}), "investigation_stop": state["finish"], "tool_errors": state["errors"], "verified_answer_points": supported, "findings": [item.model_dump() for item in draft.findings], "finding_reviews": [item.model_dump() for item in checked_findings.values()], "update_reviews": [item.model_dump() for item in checked_updates.values()], "draft_uncertainties": draft.uncertainties, "instruction": "这里只检查已核验回答是否满足用户完成标准，并整理仍成立的实际知识缺口。不能新增事实。剔除已被原文反驳的草稿缺口；不得把分批输入说成整个任务遗漏。调查因额度停止时，仍依据实际核验内容检查完成标准；需要调查最新变化却没有执行来源检查时不能视为完成。用户要求说明未知条件时，可以通过明确说明缺口完成该要求。调用 complete_research_review。"}
    payload["sources"] = [{key: chunk[key] for key in ("chunk_id", "document_id", "version_id", "title", "url", "extent")} for chunk in chunks.values()]
    payload["citation_policy"] = "服务器根据 finding_refs 与 chunk_ids 自动在每个回答要点后提供 sources 中的原始 URL 和保存原文链接，模型无需在 text 字段重复链接。对本机已有原文进行整理时，提供既有链接不要求重新访问网页。"
    payload["coverage"] = run.coverage_status() if run.run else None
    if run.run:
        payload["instruction"] += "行业检查可以没有新增事实；请根据 coverage 中实际成功的查询、检查时间及未覆盖对象核对完成标准。覆盖完整且没有新变化时 complete=true，说明本次无新增；覆盖不足或失败时 complete=false。不能为形成回答重复已有知识。"
    identity = continuity.digest({key: payload[key] for key in ("findings", "finding_reviews", "update_reviews", "verified_answer_points")})
    payload = await prepare_payload(run, f"completion_review:{PROMPT_VERSION}:{identity}", payload)
    operation_key = "completion_review:" + hashlib.sha256(station.encode(payload).encode()).hexdigest()[:24]
    cached = db.one("SELECT id FROM research_operations WHERE task_id=? AND operation_key=? AND status='success'", (run.task["id"], operation_key))
    if run.gateway.remaining() or cached:
        completion = await submit(run, payload, CompletionReview, "complete_research_review", "completion_review")
    else:
        completion = CompletionReview(complete=False, notes="本次额度不足以完成整体完成标准检查，已保留通过核验的内容。", uncertainties=["整体完成标准检查尚未完成。"])
    result = Review(findings=list(checked_findings.values()), points=list(checked_points.values()), updates=list(checked_updates.values()), completion_satisfied=completion.complete and not deferred, completion_notes=completion.notes, uncertainties=completion.uncertainties, deferred_refs=deferred)
    run.record("逐项证据核验", outputs=result.model_dump())
    return {"review": result.model_dump()}
