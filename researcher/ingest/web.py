import hmac
import json
import sqlite3
from datetime import datetime, timezone
from typing import Annotated, Literal

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import Field, TypeAdapter, ValidationError

from .. import db, station
from ..industry import selection
from ..providers import ResearchError
from ..config import MODEL_STEPS, STEP_LABELS, settings
from ..web_ids import OptionalRecordId, RecordId, parse_record_id
from . import benchmark, jobs
from .cluster import correct_assignment, update_event
from .collectors import SOURCE_HAS_HISTORY, FeedConfig, change_source_lifecycle, ensure_catalog, save_source, source_config
from .funnel import ITEM_ERRORS
from .gate import BODY_GATE_LABELS, fetch_error, preview

STATES = {"pending_date": "等待可靠日期", "gated_archive": "闸门归档", "discovered": "等待评估", "prefiltered_out": "预筛淘汰",
          "scored_out": "得分不足", "selected": "等待聚簇", "clustered": "已归入事件", "duplicate": "重复链接"}
REASONS = {"missing_date": "缺少可靠发布日期", "future_date": "发布日期晚于检查时间", "published_after": "早于或等于监控起点",
           "expired": "首次发现时已超过时效窗口", "initial_backfill": "首次导入存量", "before_backfill_window": "超出首次导入范围",
           "known_url": "已发现此链接", "same_document": "转向已保存的同一文档", "historical_document": "已归档的历史文档", **BODY_GATE_LABELS}
SCORE_VALUE = TypeAdapter(Annotated[float, Field(ge=0, le=100, allow_inf_nan=False)])
SELECTION_RETRY_NOTICE = "当前仍未公开展示为入选事件，请点击下方「继续处理」以重新进入入选和聚簇流程。"
REVIEW_ACTIONS = {"release": "送回评估", "score": "人工评分", "assignment": "人工归属"}


def saved_sources(archived=None):
    return db.query(f"""
        SELECT s.*,e.name AS owner_name,{SOURCE_HAS_HISTORY} AS has_history,
               (SELECT count(*) FROM feed_observations o WHERE o.source_id=s.id) AS item_count,
               (SELECT count(*) FROM feed_observations o JOIN feed_items i ON i.id=o.item_id
                WHERE o.source_id=s.id AND i.state='clustered') AS selected_count,
               (SELECT error FROM feed_runs r WHERE r.source_id=s.id ORDER BY id DESC LIMIT 1) AS latest_error
        FROM feed_sources s LEFT JOIN entities e ON e.id=s.owner_entity_id
        WHERE ? IS NULL OR (s.archived_at IS NOT NULL)=? ORDER BY s.enabled DESC,s.tier,s.name
    """, (archived, archived))


def config_from_form(form):
    if "source" in form:
        return FeedConfig.model_validate_json(str(form["source"]))
    return FeedConfig.model_validate(source_fields(form, parse_options=True))


def source_fields(form, *, parse_options=False):
    draft = {key: str(form.get(key, "")).strip() for key in ("key", "name", "kind", "tier", "url", "publisher", "owner", "category", "published_after")}
    draft["publisherUrlPrefixes"] = [value.strip() for value in str(form.get("publisher_prefixes", "")).splitlines() if value.strip()]
    draft["enabled"] = form.get("enabled") == "yes"
    options = str(form.get("options", "")).strip() or "{}"
    draft["options"] = json.loads(options) if parse_options else options
    return draft


def message(error):
    if isinstance(error, ValidationError):
        return "配置校验未通过：" + "、".join(".".join(str(part) for part in issue["loc"]) + " " + issue["msg"] for issue in error.errors(include_input=False)[:3])
    return str(error)[:500]


def register(app, page, form_data, redirect, required, text):
    router = APIRouter()

    def source_page(request, draft=None, preview_rows=None, error=None, edit_id=0, archived=False):
        blank = {"key": "", "name": "", "kind": "rss", "tier": "T2", "url": "", "publisher": "", "category": "", "enabled": False}
        return page(request, "admin_sources.html", title="信源管理", sources=saved_sources(archived), edit_id=edit_id, archived=archived,
                    draft=draft or blank,
                    preview_rows=preview_rows, states=STATES, reasons=REASONS, error=error or request.query_params.get("error"))

    @router.get("/admin/sources")
    def sources(request: Request, edit: OptionalRecordId = None, archived: bool = False):
        ensure_catalog()
        selected = required("SELECT * FROM feed_sources WHERE id=?", (edit,)) if edit else None
        if selected and selected["archived_at"]:
            return redirect("/admin/sources?archived=true", error="请先恢复已归档信源，再编辑配置")
        draft = source_config(selected).model_dump() if selected else None
        if draft:
            draft["options"] = source_config(selected).options.model_dump_json(indent=2, exclude_defaults=True)
        return source_page(request, draft, edit_id=edit, archived=archived)

    @router.post("/admin/sources/preview")
    async def preview_source(request: Request, form=Depends(form_data)):
        edit_id = 0
        wants_json = "application/json" in request.headers.get("accept", "")
        try:
            source = config_from_form(form)
            edit_id = parse_record_id(form["source_id"]) if form.get("source_id") else 0
            current = db.one("SELECT * FROM feed_sources WHERE id=? AND kind=? AND url=?", (edit_id, source.kind, source.url)) if edit_id else None
            payload = json.loads(str(form.get("entries") or "[]")) if source.kind == "external" else None
            rows = await preview(source, payload, current)
        except ITEM_ERRORS as error:
            if isinstance(error, (httpx.HTTPError, OSError)):
                code, detail = "list_request_failed", f"列表页请求失败（{fetch_error(error)}）"
            else:
                code, detail = "configuration_or_format", message(error)
            return JSONResponse({"error": detail, "code": code}, status_code=422) if wants_json else source_page(request, source_fields(form), error=detail, edit_id=edit_id)
        if wants_json:
            return {"items": rows}
        return source_page(request, source_fields(form), preview_rows=rows, edit_id=edit_id)

    @router.post("/admin/sources")
    def edit_source(form=Depends(form_data)):
        try:
            source_id = parse_record_id(form["source_id"]) if form.get("source_id") else None
            identifier = save_source(config_from_form(form), source_id)
            return redirect(f"/admin/sources?edit={identifier}", "信源配置已保存")
        except sqlite3.IntegrityError:
            return redirect("/admin/sources", error="信源标识已存在")
        except ValueError as error:
            return redirect("/admin/sources", error=message(error))

    @router.post("/admin/sources/{source_id}/push")
    async def push_source(request: Request, source_id: RecordId):
        token = request.cookies.get("researcher_csrf", "")
        supplied = request.headers.get("x-researcher-csrf", "")
        if not token or not hmac.compare_digest(token.encode(), supplied.encode()):
            raise HTTPException(403, "推送需要当前会话的操作验证值")
        source = required("SELECT * FROM feed_sources WHERE id=? AND kind='external' AND enabled=1 AND archived_at IS NULL", (source_id,))
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 300000:
                raise HTTPException(413, "推送内容超过大小限制")
        try:
            return jobs.submit_external(source, json.loads(body))
        except ValueError as error:
            return JSONResponse({"error": message(error)}, status_code=422)

    @router.post("/admin/sources/{source_id}/{action}")
    def source_lifecycle(source_id: RecordId, action: Literal["archive", "restore", "delete"], form=Depends(form_data)):
        path = "/admin/sources?archived=true" if action == "restore" else "/admin/sources"
        try:
            change_source_lifecycle(source_id, action)
        except ValueError as error:
            return redirect(path, error=message(error))
        notices = {"archive": "信源已归档", "restore": "信源已恢复为停用状态", "delete": "信源已删除"}
        return redirect("/admin/sources", notices[action])

    @router.get("/admin/funnel")
    def funnel(request: Request, q: str = "", state: str = "", source_id: OptionalRecordId = None, item_id: OptionalRecordId = None):
        conditions = ["i.title LIKE ?"]
        args = [f"%{q[:150]}%"]
        if state in STATES:
            conditions.append("i.state=?")
            args.append(state)
        if source_id:
            conditions.append("i.source_id=?")
            args.append(source_id)
        rows = db.query("SELECT i.*,s.name AS source_name FROM feed_items i JOIN feed_sources s ON s.id=i.source_id WHERE " + " AND ".join(conditions) + " ORDER BY i.id DESC LIMIT 150", tuple(args))
        item = required("SELECT * FROM feed_items WHERE id=?", (item_id,)) if item_id else None
        evaluations = db.query("SELECT e.*,count(u.id) AS calls,sum(u.input_tokens) AS input_tokens,sum(u.output_tokens) AS output_tokens,"
                               "sum(u.estimate) AS estimate,group_concat(DISTINCT u.model) AS models FROM item_evaluations e "
                               "LEFT JOIN usage u ON u.evaluation_id=e.id WHERE e.item_id=? GROUP BY e.id ORDER BY e.id", (item_id,)) if item else []
        decisions = db.query("SELECT r.*,e.title FROM event_cluster_decisions r JOIN events e ON e.id=r.final_event_id WHERE r.item_id=? ORDER BY r.id DESC", (item_id,)) if item else []
        reviews = db.query("SELECT * FROM feed_manual_reviews WHERE item_id=? ORDER BY id DESC", (item_id,)) if item else []
        for row in decisions:
            row["candidates"] = station.decode(row["candidates_json"])
        policy = selection()
        for row in rows:
            row["selection_retry"] = jobs.needs_selection_retry(row, policy)
        return page(request, "admin_funnel.html", title="漏斗诊断", items=rows, item=item, evaluations=evaluations, decisions=decisions, reviews=reviews, review_actions=REVIEW_ACTIONS,
                    states=STATES, reasons=REASONS, sources=saved_sources(), q=q, state=state, source_id=source_id,
                    threshold=policy.threshold(item["tier"], item["category"]) if item else None,
                    selection_retry=bool(item and jobs.needs_selection_retry(item, policy)), selection_retry_notice=SELECTION_RETRY_NOTICE,
                    can_release=bool(item and jobs.can_release(item)))

    @router.post("/admin/funnel/{item_id}/score")
    def correct_score(item_id: RecordId, form=Depends(form_data)):
        path = f"/admin/funnel?item_id={item_id}"
        try:
            score = SCORE_VALUE.validate_python(form.get("score"))
            reason = text(form, "reason", 500)
            policy = selection()
            with db.transaction() as connection:
                item = connection.execute("SELECT * FROM feed_items WHERE id=? AND state!='duplicate'", (item_id,)).fetchone()
                if not item:
                    raise ValueError("条目不存在或已判定为重复链接")
                state = item["state"]
                if state not in {"selected", "clustered"} or score < policy.threshold(item["tier"], item["category"]):
                    state = "scored_out"
                connection.execute("UPDATE feed_items SET manual_score=?,manual_reason=?,state=?,updated_at=? WHERE id=?", (score, reason, state, db.now(), item_id))
                jobs.record_review(connection, item_id, "score", reason, score=score)
                if item["event_id"]:
                    update_event(connection, item["event_id"], datetime.now(timezone.utc), policy)
                selection_retry = jobs.needs_selection_retry({**item, "state": state, "manual_score": score}, policy)
            return redirect(path, "人工评分已保存，" + SELECTION_RETRY_NOTICE if selection_retry else "人工评分已保存")
        except ValueError as error:
            return redirect(path, error=message(error))

    @router.post("/admin/funnel/{item_id}/assign")
    def assign_item(item_id: RecordId, form=Depends(form_data)):
        path = f"/admin/funnel?item_id={item_id}"
        try:
            correct_assignment(item_id, parse_record_id(form.get("event_id")), text(form, "reason", 500))
            item = db.one("SELECT state FROM feed_items WHERE id=?", (item_id,))
            return redirect(path, "事件归属已修正并锁定" if item["state"] == "clustered" else "已保存指定事件，条目入选后使用此归属")
        except ValueError as error:
            return redirect(path, error=message(error))

    @router.post("/admin/funnel/{item_id}/retry")
    def retry(item_id: RecordId, form=Depends(form_data)):
        try:
            reason = str(form.get("reason", "")).strip()[:500]
            return redirect(f"/tasks/{jobs.retry_item(item_id, reason, release=form.get('action') == 'release')}")
        except ValueError as error:
            return redirect(f"/admin/funnel?item_id={item_id}", error=message(error))

    @router.get("/admin/eval")
    def evaluate(request: Request):
        config = settings()
        context = {"splits": benchmark.SPLITS, "variants": benchmark.VARIANTS, "step_labels": STEP_LABELS, "gate_labels": REASONS,
                   "config": config}
        try:
            gold = benchmark.samples()
        except (ValueError, OSError) as error:
            return page(request, "admin_eval.html", title="SelectBench 评测", error=message(error), gold=[], reports={}, **context)
        reports = {}
        for split in benchmark.SPLITS:
            result = db.one("SELECT id,result_json,finished_at FROM tasks WHERE kind='selectbench' AND status IN ('success','failed') "
                            "AND json_extract(job_json,'$.split')=? AND (json_type(result_json,'$.comparison')='object' "
                            "OR json_type(result_json,'$.metrics')='object') "
                            "ORDER BY id DESC LIMIT 1", (split,))
            if result:
                report = station.decode(result["result_json"])
                comparison = report.get("comparison")
                if not isinstance(comparison, dict):
                    comparison = {"历史评测": {**report, "usage": benchmark.run_usage(result["id"])}}
                reports[split] = {**result, "comparison": comparison}
        return page(request, "admin_eval.html", title="SelectBench 评测", gold=gold, reports=reports, **context)

    @router.post("/admin/eval")
    def run_evaluation(form=Depends(form_data)):
        try:
            names = form.getlist("variant") if "compare" in form else ["custom"]
            overrides = {step: str(form.get(step, "")) for step in MODEL_STEPS}
            return redirect(f"/tasks/{benchmark.enqueue(str(form.get('split')), names, overrides)}")
        except (ValueError, OSError) as error:
            return redirect("/admin/eval", error=message(error))

    app.include_router(router)
