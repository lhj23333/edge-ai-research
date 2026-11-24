import asyncio
from datetime import date
from typing import Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .. import continuity, db, knowledge, station
from ..config import resolve_route
from ..industry import INDUSTRY, Tier, selection
from ..providers import BudgetExceeded, Gateway, ResearchError, structured_json
from .funnel import ITEM_ERRORS, Prefilter, assessment_payload, dual_score, model_request
from .jobs import task_checkpoint

SPLITS = {"development": "开发集", "holdout": "留出集"}


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
            "precision": precision, "recall": recall, "f1": 2 * tp / (2 * tp + fp + fn) if tp + fp + fn else 0}


def summarize(samples, rows, errors):
    by_tier = {tier: metrics([row for row in rows if row["tier"] == tier], sum(sample["tier"] == tier for sample in samples))
               for tier in ("T1", "T1.5", "T2")}
    return {"rows": rows, "errors": errors, "metrics": metrics(rows, len(samples)), "by_tier": by_tier}


def enqueue(split):
    if split not in SPLITS:
        raise ValueError("请选择开发集或留出集")
    values = [row for row in samples() if row["split"] == split]
    if not values:
        raise ValueError("该样本集为空")
    for step in ("prefilter", "score"):
        resolve_route(step)
    with db.transaction() as connection:
        active = connection.execute(
            "SELECT id FROM tasks WHERE kind='selectbench' AND status IN ('queued','running') "
            "AND cancel_requested=0 AND json_extract(job_json,'$.split')=? ORDER BY id LIMIT 1", (split,),
        ).fetchone()
        if active:
            return active[0]
        return connection.execute("INSERT INTO tasks(kind,prompt,profile_id,profile_fingerprint,provider,model,created_at,job_json,priority,request_limit) "
                                  "VALUES('selectbench',?,'','','','',?,?,-8,?)",
                                  ("SelectBench " + SPLITS[split], db.now(), station.encode({"split": split, "samples": values}), 3 * len(values))).lastrowid


async def perform(task):
    job = station.decode(task["job_json"])
    policy = selection()
    task_id = task["id"]
    semaphore = asyncio.Semaphore(2)
    budget_error = None
    async with httpx.AsyncClient(timeout=httpx.Timeout(30, connect=8), trust_env=False) as client:
        for index, sample in enumerate(job["samples"]):
            step = f"样本 {index + 1}"
            if db.one("SELECT id FROM task_steps WHERE task_id=? AND stage=?", (task_id, step)):
                continue
            task_checkpoint(task_id, f"正在评测 {index + 1}/{len(job['samples'])}")
            versions = {}

            async def call(stage, data, model):
                name, version, messages = model_request(stage, data, model)
                versions[stage] = version
                async with semaphore:
                    task_checkpoint(task_id, "正在调用 SelectBench 评测模型")
                    route = resolve_route(name)
                    gateway = Gateway(route.profile, task_id, route)
                    key = f"bench:{index}:{stage}:{version}"
                    reply = await gateway.complete(messages, structured=True, request_key=key, operation=stage, stream=False)
                    with gateway.validate_result(key):
                        return model.model_validate(structured_json(reply.text))

            try:
                prefilter = await call("prefilter", {key: sample[key] for key in ("title", "summary")}, Prefilter)
                score = None
                threshold = policy.threshold(sample["tier"], sample["category"])
                missing_date = "published_at" in sample and sample["published_at"] is None
                if prefilter.keep and not missing_date:
                    resource = await knowledge.get_resource(client, sample["url"])
                    content, _, _, _ = await asyncio.to_thread(knowledge.extract_resource, resource.body, resource.headers.get("content-type", "text/html"), resource.url)
                    score = await dual_score(call, assessment_payload({**sample, "canonical_url": sample["url"], "published_at": sample.get("published_at")}, content))
                result = {**sample, "prefilter": prefilter.keep, "score": score, "threshold": threshold,
                          "selected": score is not None and score >= threshold, "versions": versions,
                          "gate_reason": "missing_date" if prefilter.keep and missing_date else ""}
                station.record_step(task_id, step, "success", outputs=result)
            except BudgetExceeded as error:
                budget_error = str(error)
                break
            except ITEM_ERRORS as exc:
                detail = str(exc) if isinstance(exc, ResearchError) else type(exc).__name__
                station.record_step(task_id, step, "failed", outputs={**sample, "error": detail})
    outcomes = db.query("SELECT status,outputs_json FROM task_steps WHERE task_id=? ORDER BY id", (task_id,))
    rows = [station.decode(row["outputs_json"]) for row in outcomes if row["status"] == "success"]
    errors = [station.decode(row["outputs_json"]) for row in outcomes if row["status"] == "failed"]
    report = summarize(job["samples"], rows, errors)
    status = "failed" if budget_error else "success"
    progress = "评测未完成" if budget_error else "已生成评测结果"
    db.execute("UPDATE tasks SET status=?,progress=?,error=?,result_text=?,result_json=?,finished_at=? WHERE id=? AND cancel_requested=0",
               (status, progress, budget_error or "", f"完成 {len(rows)}/{len(job['samples'])} 条样本。", station.encode(report), db.now(), task_id))
