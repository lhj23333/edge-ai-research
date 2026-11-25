import asyncio
from datetime import datetime, timedelta, timezone
from typing import Literal

import httpx
from lxml import etree
from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, model_validator
from pypdf.errors import PdfReadError

from .. import continuity, db, knowledge, station
from ..config import resolve_route
from ..industry import prompt
from ..providers import BudgetExceeded, Gateway, ResearchError, UnknownOutcome, completion_choice, structured_json, yield_to_manual
from .collectors import article, article_date, detail_date, matches_prefix, parse_date, source_config, stamp
from .gate import body_gate, existing_item, known_document, processing_source, publication_state, publisher_for, publishers

DOCUMENT_CHARS = 10000
SCORE_STAGES = ("score_1", "score_2")


class Assessment(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reason: str = Field(min_length=1, max_length=500)


class Prefilter(Assessment):
    keep: bool


class Score(Assessment):
    score: float = Field(ge=0, le=100, allow_inf_nan=False)


class NamedEntity(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["厂商", "产品", "应用场景", "技术路线", "软件生态"]
    name: str = Field(min_length=2, max_length=120)


class Writeup(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str = Field(min_length=2, max_length=180)
    summary: str = Field(min_length=2, max_length=700)
    importance_reason: str = Field(min_length=1, max_length=700)
    event_type: str = Field(min_length=1, max_length=80)
    version: str = Field(default="", max_length=80)
    entities: list[NamedEntity] = Field(default_factory=list, max_length=8)
    topic_ids: list[int] = Field(default_factory=list, max_length=5)

    @model_validator(mode="after")
    def validate_category(self, info: ValidationInfo):
        if info.context and self.event_type not in info.context["event_types"]:
            raise ValueError("模型返回了行业配置中不存在的事件类别")
        return self


def model_request(stage, payload, result_model):
    route = "score" if stage in SCORE_STAGES else "cluster" if stage in {"write", "cluster_review"} else stage
    instruction = prompt("write" if stage == "write" else route)
    schema = result_model.model_json_schema()
    version = continuity.digest([instruction, schema, "正文摘要"] if stage == "write" else [instruction, schema])[:20]
    messages = [{"role": "system", "content": instruction},
                {"role": "user", "content": station.encode({"data": payload, "output_schema": schema})}]
    return route, version, messages


def assessment_row(item_id, stage, version, route, task_id, messages):
    with db.transaction() as connection:
        connection.execute("INSERT OR IGNORE INTO item_evaluations(item_id,stage,prompt_version,route,task_id,request_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                           (item_id, stage, version, route, task_id, station.encode(messages), db.now(), db.now()))
        return dict(connection.execute("SELECT * FROM item_evaluations WHERE item_id=? AND stage=? AND prompt_version=?", (item_id, stage, version)).fetchone())


def save_assessment(row, value):
    result = value.model_dump()
    db.execute("UPDATE item_evaluations SET status='success',score=?,reason=?,result_json=?,updated_at=? WHERE id=?",
               (result.get("score"), result.get("reason", ""), station.encode(result), db.now(), row["id"]))
    return value


def stored_response(row, result_model):
    previous = db.one("SELECT status,outputs_json FROM research_operations WHERE task_id=? AND operation_key=?", (row["task_id"], f"feed:{row['id']}"))
    if not previous or previous["status"] in {"blocked", "retry_authorized"}:
        return None
    if previous["status"] == "success":
        choice = completion_choice(station.decode(previous["outputs_json"]))
        if choice.get("finish_reason") == "length":
            raise ResearchError("模型输出达到上限，需要人工继续处理")
        return parse_assessment(row, result_model, choice["message"]["content"])
    if previous["status"] in {"unknown", "running"}:
        raise UnknownOutcome("此前评分请求的结果未知，需要人工继续处理")
    raise ResearchError("此前模型结果未通过校验或请求失败，需要人工继续处理")


def parse_assessment(row, result_model, text):
    messages = station.decode(row["request_json"])
    context = station.decode(messages[1]["content"])["data"]
    return result_model.model_validate(structured_json(text), context=context)


async def evaluate(task_id, item_id, stage, payload, result_model, semaphore):
    route, version, messages = model_request(stage, payload, result_model)
    row = assessment_row(item_id, stage, version, route, task_id, messages)
    if row["status"] == "success":
        return result_model.model_validate_json(row["result_json"])
    if row["status"] in {"failed", "unknown"}:
        raise ResearchError(row["reason"])
    try:
        saved = stored_response(row, result_model)
    except (ValueError, ResearchError, KeyError, TypeError) as error:
        record_evaluation_error(row, error)
        if isinstance(error, ResearchError):
            raise
        raise ResearchError("已保存的模型返回内容未通过校验") from error
    if saved is not None:
        return save_assessment(row, saved)
    async with semaphore:
        yield_to_manual()
        effective = resolve_route(route)
        gateway = Gateway(effective.profile, task_id, effective, row["id"])
        key = f"feed:{row['id']}"
        db.execute("UPDATE item_evaluations SET task_id=?,updated_at=? WHERE id=?", (task_id, db.now(), row["id"]))
        try:
            reply = await gateway.complete(station.decode(row["request_json"]), structured=True, request_key=key, operation=stage)
            with gateway.validate_result(key):
                result = parse_assessment(row, result_model, reply.text)
        except BudgetExceeded:
            raise
        except (ValueError, ResearchError, KeyError, TypeError) as error:
            record_evaluation_error(row, error)
            if isinstance(error, ResearchError):
                raise
            raise ResearchError("模型返回内容未通过校验") from error
    return save_assessment(row, result)


async def write_summary(item, content, topics, policy, call):
    payload = {"content": content[:DOCUMENT_CHARS], "tier": item["tier"], "event_types": policy.event_types, "topics": topics}
    route, version, _ = model_request("write", payload, Writeup)
    model = resolve_route(route)
    namespace = "summary:" + continuity.digest([version, model.profile.provider, str(model.profile.base_url),
                                                  model.profile.model, model.reasoning_effort, model.output_tokens,
                                                  {key: value for key, value in payload.items() if key != "content"}])
    content_hash = continuity.text_hash(content)
    cached = continuity.cache_get(namespace, content_hash)
    if cached is not None:
        return Writeup.model_validate_json(cached, context=payload)
    summary = await call("write", payload, Writeup)
    continuity.cache_put(namespace, content_hash, summary.model_dump_json())
    return summary


def reject_body(item, reason):
    db.execute("UPDATE feed_items SET state='prefiltered_out',gate_reason=?,updated_at=?,error='',next_attempt_at=NULL "
               "WHERE id=? AND state=? AND manual_score IS NULL", (reason, db.now(), item["id"], item["state"]))


def record_evaluation_error(row, error):
    status = "unknown" if isinstance(error, UnknownOutcome) else "failed"
    reason = str(error) if isinstance(error, ResearchError) else "结构化模型返回内容未通过校验"
    db.execute("UPDATE item_evaluations SET status=?,reason=?,updated_at=? WHERE id=?", (status, reason, db.now(), row["id"]))


def average_score(first, second):
    return (first.score + second.score) / 2


async def dual_score(call, payload):
    results = await asyncio.gather(*(call(stage, payload, Score) for stage in SCORE_STAGES), return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException):
            raise result
    return average_score(*results)


def assessment_payload(item, content):
    return {"title": item["title"], "url": item["canonical_url"], "published_at": item["published_at"],
            "tier": item["tier"], "category": item["category"], "content": content[:DOCUMENT_CHARS]}


async def resolve_publication(item, source, client, policy):
    published = parse_date(item["published_at"])
    if published is None:
        published = await article_date(client, source_config(source), item["canonical_url"])
    checked = datetime.now(timezone.utc)
    with db.transaction() as connection:
        current = connection.execute("SELECT * FROM feed_items WHERE id=?", (item["id"],)).fetchone()
        if current["state"] != "pending_date":
            item.update(current)
            return
        published = parse_date(current["published_at"]) or published
        state, reason = publication_state(connection, current, published, checked, policy)
        update = {"published_at": stamp(published) if published else None, "state": state, "gate_reason": reason}
        connection.execute("UPDATE feed_items SET published_at=?,state=?,gate_reason=?,error='',next_attempt_at=?,updated_at=? WHERE id=?",
                           (*update.values(), stamp(checked + timedelta(hours=6)) if state == "pending_date" else None, stamp(checked), item["id"]))
    item.update(update)


async def save_original(item, source, task_id, client, resource, policy):
    known = continuity.document_for_url(item["canonical_url"])
    checked = db.one("SELECT * FROM source_checks WHERE document_id=?", (known["id"],)) if known else None
    config = source_config(source)
    if resource is None:
        try:
            resource = await article(client, config, item["canonical_url"], checked)
        except (httpx.HTTPError, OSError) as error:
            if known:
                knowledge.record_fetch_failure(known["id"], error)
            raise
        if resource.status == 304:
            knowledge.mark_unchanged(checked["document_id"])
        declared = parse_date(known["published_at"]) if resource.status == 304 else await asyncio.to_thread(detail_date, resource, config)
        published = parse_date(item["published_at"])
        if declared and published and declared < published:
            with db.transaction() as connection:
                current = connection.execute("SELECT * FROM feed_items WHERE id=?", (item["id"],)).fetchone()
                if current["state"] != item["state"]:
                    return None
                state, reason = publication_state(connection, current, declared, datetime.now(timezone.utc), policy)
                if state == "discovered" and item["state"] == "selected":
                    state = "selected"
                item.update(published_at=stamp(declared), state=state, gate_reason=reason)
                connection.execute("UPDATE feed_items SET published_at=?,state=?,gate_reason=?,updated_at=? WHERE id=?",
                                   (item["published_at"], state, reason, db.now(), item["id"]))
            if state not in {"discovered", "selected"}:
                return None
    canonical = continuity.canonical_url(resource.url)
    with db.transaction() as connection:
        known_publishers = publishers(connection)
        owner = publisher_for(canonical, source, known_publishers)
        effective_tier = owner["tier"]
        host = httpx.URL(canonical).host.removeprefix("www.")
        same_publisher = (source["url"] and host == httpx.URL(source["url"]).host.removeprefix("www.")) or any(matches_prefix(canonical, prefix) for prefix, _ in known_publishers)
        if effective_tier == "T1" and not same_publisher:
            effective_tier = "T2"
        publisher_key = owner["publisher_key"] if same_publisher else host
        prior = existing_item(connection, canonical, known_document(connection, canonical, resource.url))
        if prior and prior["id"] != item["id"]:
            connection.execute("INSERT OR IGNORE INTO feed_observations(source_id,item_id,seen_at) SELECT source_id,?,seen_at FROM feed_observations WHERE item_id=?", (prior["id"], item["id"]))
            connection.execute("DELETE FROM feed_observations WHERE item_id=?", (item["id"],))
            connection.execute("""
                UPDATE feed_items SET state='duplicate',gate_reason='same_document',document_id=?,version_id=(
                    SELECT v.id FROM document_versions v JOIN documents d ON d.id=v.document_id
                    WHERE v.document_id=? ORDER BY (v.content_hash=d.content_hash) DESC,v.id DESC LIMIT 1
                ),event_id=?,updated_at=? WHERE id=?
            """, (prior["document_id"], prior["document_id"], prior["event_id"], db.now(), item["id"]))
            return None
    if resource.status == 304:
        document_id, version_id = checked["document_id"], checked["version_id"]
        content = db.one("SELECT content FROM document_versions WHERE id=?", (version_id,))["content"]
    else:
        document_id, version_id, content = await asyncio.to_thread(knowledge.archive_resource, resource, item["canonical_url"], task_id,
                                                                    item["published_at"], owner["name"], item["title"])
    attribution = (effective_tier, owner["category"], publisher_key, owner["owner_entity_id"] if effective_tier != "T2" else None)
    with db.transaction() as connection:
        connection.execute("UPDATE feed_items SET canonical_url=?,document_id=?,version_id=?,tier=?,category=?,publisher_key=?,owner_entity_id=?,updated_at=? WHERE id=?",
                           (canonical, document_id, version_id, *attribution, db.now(), item["id"]))
        station.link_document(connection, document_id, task_id, version_id)
    item.update(canonical_url=canonical, document_id=document_id, version_id=version_id, tier=attribution[0], category=attribution[1], publisher_key=attribution[2], owner_entity_id=attribution[3])
    return content


async def process_item(task_id, item, client, policy, semaphore):
    from .cluster import assign
    yield_to_manual()
    with db.open_database() as connection:
        source = processing_source(connection, item["id"])
    if not source or item["state"] not in {"pending_date", "discovered", "selected"}:
        return

    async def call(stage, payload, model):
        return await evaluate(task_id, item["id"], stage, payload, model, semaphore)

    if item["manual_score"] is None and (item["state"] == "pending_date" or
                                         (item["state"] == "discovered" and (not item["version_id"] or item["manual_release"]))):
        prefilter = await call("prefilter", {"title": item["title"], "summary": item["summary"][:400]}, Prefilter)
        if not prefilter.keep:
            db.execute("UPDATE feed_items SET state='prefiltered_out',updated_at=?,error='',next_attempt_at=NULL "
                       "WHERE id=? AND state=? AND manual_score IS NULL", (db.now(), item["id"], item["state"]))
            return
        yield_to_manual()
    if item["state"] == "pending_date":
        await resolve_publication(item, source, client, policy)
        if item["state"] != "discovered":
            return
        with db.open_database() as connection:
            source = processing_source(connection, item["id"])
        if not source:
            return

    if item["version_id"]:
        with db.transaction() as connection:
            content = connection.execute("SELECT content FROM document_versions WHERE id=?", (item["version_id"],)).fetchone()["content"]
            station.link_document(connection, item["document_id"], task_id, item["version_id"])
    else:
        try:
            content = await save_original(item, source, task_id, client, None, policy)
        except knowledge.EmptyBody:
            if item["manual_score"] is not None:
                raise
            reject_body(item, "empty_body")
            return
        if content is None:
            return
    if item["state"] == "discovered":
        score = item["score"]
        if score is None and item["manual_score"] is None:
            reason = body_gate(content)
            if reason:
                reject_body(item, reason)
                return
            score = await dual_score(call, assessment_payload(item, content))
        with db.transaction() as connection:
            current = connection.execute("SELECT state,manual_score,manual_target_event_id FROM feed_items WHERE id=?", (item["id"],)).fetchone()
            if current["state"] != "discovered":
                return
            effective = current["manual_score"] if current["manual_score"] is not None else score
            state = "selected" if effective >= policy.threshold(item["tier"], item["category"]) else "scored_out"
            connection.execute("UPDATE feed_items SET state=?,score=?,updated_at=?,error='',next_attempt_at=NULL WHERE id=?",
                               (state, score, db.now(), item["id"]))
        if state == "scored_out":
            return
        item.update(current)
        item.update(state=state, score=score)

    topics = db.query("SELECT id,title,question FROM topics WHERE research_state='active' ORDER BY id LIMIT 30")
    summary = await write_summary(item, content, topics, policy, call)
    supported = {value for value in (entity.name for entity in summary.entities) if station.normalized(value) in station.normalized(item["title"] + " " + content)}
    summary = summary.model_copy(update={"entities": [entity for entity in summary.entities if entity.name in supported],
                                         "topic_ids": sorted(set(summary.topic_ids) & {topic["id"] for topic in topics})})
    yield_to_manual()
    await assign(item, summary, call, policy)


ITEM_ERRORS = (httpx.HTTPError, OSError, ValueError, ResearchError, etree.LxmlError, PdfReadError)


def item_failure(item_id, error):
    when = datetime.now(timezone.utc) + timedelta(minutes=15)
    failed = db.one("SELECT id FROM item_evaluations WHERE item_id=? AND status IN ('failed','unknown') LIMIT 1", (item_id,))
    db.execute("UPDATE feed_items SET error=?,next_attempt_at=?,updated_at=? WHERE id=?",
               (str(error)[:500] if isinstance(error, ResearchError) or type(error) is ValueError else type(error).__name__,
                "9999-12-31T00:00:00+00:00" if failed else stamp(when), db.now(), item_id))
