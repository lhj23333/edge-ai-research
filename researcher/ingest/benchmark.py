import asyncio
from datetime import date
from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .. import continuity, db, knowledge, station
from ..config import COST_PROFILES, MODEL_STEPS, EffectiveRoute, get_profile, model_routes, resolve_route, routes_fingerprint, settings
from ..industry import INDUSTRY, Tier, selection
from ..providers import BudgetExceeded, Gateway, ResearchError, structured_json
from .funnel import ITEM_ERRORS, Prefilter, assessment_payload, dual_score, model_request
from .gate import body_gate
from .jobs import task_checkpoint

SPLITS = {"development": "开发集", "holdout": "留出集"}
VARIANTS = {**{key: value.label for key, value in COST_PROFILES.items()}, "custom": "自定义"}


class GoldSample(BaseModel):
    model_config = ConfigDict(extra="forbid")

    split: Literal["development", "holdout"]
    group: str = Field(min_length=1)
    title: str = Field(min_length=1, max_length=400)
    url: str
    summary: str = Field(default="", max_length=400)
    tier: Tier
    category: str
    published_at: date | None = None
    expected: bool
    note: str = ""

    @field_validator("url")
    @classmethod
    def url_key(cls, value):
        return continuity.canonical_url(value)


def samples():
    groups, urls, rows = {}, set(), []
    for line in (INDUSTRY / "gold.jsonl").read_text().splitlines():
        if not line.strip():
            continue
        sample = GoldSample.model_validate_json(line)
        if sample.url in urls or groups.setdefault(sample.group, sample.split) != sample.split:
            raise ValueError("SelectBench 数据包含重复链接，或同一事件同时出现在两个样本集")
        urls.add(sample.url)
        rows.append(sample.model_dump(mode="json"))
    return rows


def metrics(rows, total):
    tp = sum(row["expected"] and row["selected"] for row in rows)
    fp = sum(not row["expected"] and row["selected"] for row in rows)
    fn = sum(row["expected"] and not row["selected"] for row in rows)
    precision = tp / (tp + fp) if tp + fp else 0
    recall = tp / (tp + fn) if tp + fn else 0
    return {"total": total, "tested": len(rows), "coverage": len(rows) / total if total else 0,
            "accuracy": sum(row["expected"] == row["selected"] for row in rows) / len(rows) if rows else 0,
            "precision": precision, "recall": recall, "f1": 2 * tp / (2 * tp + fp + fn) if tp + fp + fn else 0}


def summarize(samples, rows, errors):
    by_tier = {tier: metrics([row for row in rows if row["tier"] == tier], sum(sample["tier"] == tier for sample in samples))
               for tier in ("T1", "T1.5", "T2")}
    return {"rows": rows, "errors": errors, "metrics": metrics(rows, len(samples)), "by_tier": by_tier}


def configurations(names, overrides):
    config = settings()
    result = {}
    for name in names:
        if name not in VARIANTS:
            raise ValueError("请选择有效的评测档位")
        routes = model_routes({step: {"profile_id": overrides[step]} if overrides.get(step) else config["model_routes"][step]
                               for step in MODEL_STEPS}) if name == "custom" else model_routes({})
        run = {**config, "cost_profile": config["cost_profile"] if name == "custom" else name, "model_routes": routes}
        effective = [resolve_route(step, config=run) for step in MODEL_STEPS]
        result[name] = {"routes": [{"step": route.step, "id": route.profile.id, "effort": route.reasoning_effort,
                                    "tokens": route.output_tokens, "model": route.profile.model} for route in effective],
                        "fingerprint": routes_fingerprint(effective[:2])}
    return result


def enqueue(split, names=("custom",), overrides=None):
    if split not in SPLITS:
        raise ValueError("请选择开发集或留出集")
    values = [row for row in samples() if row["split"] == split]
    if not values:
        raise ValueError("该样本集为空")
    runs = configurations(names, overrides or {})
    if not runs:
        raise ValueError("请至少选择一组评测配置")
    signature = continuity.digest(runs)
    with db.transaction() as connection:
        active = connection.execute(
            "SELECT id FROM tasks WHERE kind='selectbench' AND status IN ('queued','running') "
            "AND cancel_requested=0 AND json_extract(job_json,'$.split')=? AND json_extract(job_json,'$.signature')=? ORDER BY id LIMIT 1",
            (split, signature),
        ).fetchone()
        if active:
            return active[0]
        return connection.execute("INSERT INTO tasks(kind,prompt,profile_id,profile_fingerprint,provider,model,created_at,job_json,priority,request_limit) "
                                  "VALUES('selectbench',?,'','','','',?,?,-8,?)",
                                  ("SelectBench " + SPLITS[split], db.now(), station.encode(
                                      {"split": split, "samples": values, "runs": runs, "signature": signature}),
                                   9 * len(values) * len(runs))).lastrowid


def run_routes(spec):
    config = settings()
    routes = [EffectiveRoute(row["step"], get_profile(row["id"], config), row["effort"], row["tokens"]) for row in spec["routes"][:2]]
    if routes_fingerprint(routes) != spec["fingerprint"]:
        raise ResearchError("评测模型配置已变化，请重新发起评测")
    return {route.step: route for route in routes}


def run_usage(task_id, name=None):
    pattern = f"bench:{name}:%" if name is not None else None
    return db.one("""
        SELECT count(*) AS calls,
               coalesce(sum(coalesce(input_tokens,estimated_input_tokens)),0) AS input_tokens,
               coalesce(sum(coalesce(output_tokens,estimated_output_tokens)),0) AS output_tokens,
               sum(estimated_cost) AS cost,
               count(*) FILTER (WHERE estimated_cost IS NULL) AS unknown_cost
        FROM model_call_ledger WHERE task_id=? AND (? IS NULL OR request_key LIKE ?) AND status!='network_error'
    """, (task_id, pattern, pattern))


async def perform(task):
    job = station.decode(task["job_json"])
    policy = selection()
    task_id = task["id"]
    semaphore = asyncio.Semaphore(2)
    legacy = not job.get("runs")
    specifications = job.get("runs") or configurations(("custom",), {})
    routes = {name: run_routes(spec) for name, spec in specifications.items()}
    blocked = {}
    async with httpx.AsyncClient(timeout=httpx.Timeout(30, connect=8), trust_env=False) as client:
        for index, sample in enumerate(job["samples"]):
            content = None
            fetch_error = None
            for name, slots in routes.items():
                step = f"样本 {index + 1}" if legacy else f"{name} 样本 {index + 1}"
                if (name in blocked and sample.get("published_at") is not None) or db.one(
                    "SELECT id FROM task_steps WHERE task_id=? AND stage=?", (task_id, step),
                ):
                    continue
                task_checkpoint(task_id, f"正在评测{VARIANTS[name]} · {index + 1}/{len(job['samples'])}")
                versions = {}

                async def call(stage, data, model):
                    slot, version, messages = model_request(stage, data, model)
                    versions[stage] = version
                    async with semaphore:
                        route = slots[slot]
                        gateway = Gateway(route.profile, task_id, route)
                        prefix = "bench" if legacy else f"bench:{name}"
                        key = f"{prefix}:{index}:{stage}:{version}"
                        reply = await gateway.complete(messages, structured=True, request_key=key, operation=stage, stream=False)
                        with gateway.validate_result(key):
                            return model.model_validate(structured_json(reply.text))

                try:
                    reason = "missing_date" if sample.get("published_at") is None else ""
                    keep, score = False, None
                    threshold = policy.threshold(sample["tier"], sample["category"])
                    if not reason:
                        prefilter = await call("prefilter", {key: sample[key] for key in ("title", "summary")}, Prefilter)
                        keep = prefilter.keep
                    if keep:
                        if content is None and fetch_error is None:
                            try:
                                resource = await knowledge.get_resource(client, sample["url"])
                                content, _, _, _ = await asyncio.to_thread(
                                    knowledge.extract_resource, resource.body, resource.headers.get("content-type", "text/html"), resource.url)
                            except knowledge.EmptyBody:
                                content = ""
                            except ITEM_ERRORS as error:
                                fetch_error = error
                        if fetch_error:
                            raise fetch_error
                        reason = body_gate(content)
                        if not reason:
                            score = await dual_score(call, assessment_payload({**sample, "canonical_url": sample["url"]}, content))
                    result = {**sample, "variant": name, "prefilter": keep, "score": score, "threshold": threshold,
                              "selected": score is not None and score >= threshold, "versions": versions, "gate_reason": reason}
                    station.record_step(task_id, step, "success", outputs=result)
                except BudgetExceeded as error:
                    blocked[name] = str(error)
                except ITEM_ERRORS as error:
                    detail = str(error) if isinstance(error, ResearchError) else type(error).__name__
                    station.record_step(task_id, step, "failed", outputs={**sample, "variant": name, "error": detail})
    outcomes = db.query("SELECT status,outputs_json FROM task_steps WHERE task_id=? ORDER BY id", (task_id,))
    rows = [station.decode(row["outputs_json"]) for row in outcomes if row["status"] == "success"]
    errors = [station.decode(row["outputs_json"]) for row in outcomes if row["status"] == "failed"]
    comparison = {}
    for name, spec in specifications.items():
        comparison[name] = {**summarize(job["samples"], [row for row in rows if legacy or row.get("variant") == name],
                                       [error for error in errors if legacy or error.get("variant") == name]),
                            "routes": spec["routes"], "usage": run_usage(task_id, None if legacy else name), "blocked": blocked.get(name)}
    report = {"comparison": comparison}
    if len(comparison) == 1:
        report.update(next(iter(comparison.values())))
    status = "failed" if blocked else "success"
    progress = "评测部分完成" if blocked else "已生成评测结果"
    db.execute("UPDATE tasks SET status=?,progress=?,error=?,result_text=?,result_json=?,finished_at=? WHERE id=? AND cancel_requested=0",
               (status, progress, " ".join(dict.fromkeys(blocked.values())), f"完成 {len(rows)}/{len(job['samples']) * len(routes)} 次样本判断。",
                station.encode(report), db.now(), task_id))
