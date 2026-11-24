import asyncio
import json
import hashlib
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import httpx
from httpx_sse import EventSource, SSEError
from markdown_it import MarkdownIt

from . import db
from .config import EffectiveRoute, Limits, secret, settings


ARK_SEARCH_ROUNDS = 3


class ResearchError(Exception):
    pass


class Cancelled(ResearchError):
    pass


class BudgetExceeded(ResearchError):
    pass


class UnknownOutcome(ResearchError):
    pass


class YieldResearch(Exception):
    pass


def yield_to_manual(connection=None):
    query = "SELECT id FROM tasks WHERE priority>0 AND status IN ('queued','running') AND cancel_requested=0 LIMIT 1"
    waiting = db.one(query) if connection is None else connection.execute(query).fetchone()
    if waiting:
        raise YieldResearch()


def cost_windows(connection, limits, timestamp, day):
    for label, limit, condition, value in (
        ("每分钟", limits.minute_cost, "created_at>?", (timestamp - timedelta(minutes=1)).isoformat(timespec="microseconds")),
        ("每小时", limits.hour_cost, "created_at>?", (timestamp - timedelta(hours=1)).isoformat(timespec="microseconds")),
        ("每日", limits.daily_cost, "day=?", day),
    ):
        if limit is not None:
            spent = connection.execute("SELECT coalesce(sum(coalesce(estimate,reserved_estimate)),0) FROM usage WHERE " + condition + " AND status!='network_error'", (value,)).fetchone()[0]
            yield label, limit, spent


def completion_choice(data):
    return (data["output"] if "output" in data else data)["choices"][0]


def error_usage(response):
    if "json" not in response.headers.get("content-type", ""):
        return {}
    try:
        data = response.json()
    except ValueError:
        return {}
    usage = data.get("usage") if isinstance(data, dict) else None
    return usage if isinstance(usage, dict) else {}


@dataclass
class Source:
    title: str
    url: str
    content: str = ""
    extent: str = "搜索摘要"
    published_at: str | None = None


@dataclass
class Reply:
    text: str
    sources: list[Source] = field(default_factory=list)
    message: dict = field(default_factory=dict)


def structured_json(text):
    tokens = MarkdownIt().parse(text)
    if len(tokens) == 1 and tokens[0].type == "fence":
        text = tokens[0].content
    return json.loads(text)


def source_url(value):
    parts = urlsplit(value)
    if parts.scheme not in ("http", "https") or not parts.hostname or parts.username is not None or parts.password is not None:
        raise ResearchError("服务商返回了无效的来源链接")
    return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))


def normalized_source(title, url, content="", extent="搜索摘要", date=None):
    return Source(str(title or url), source_url(url), str(content or ""), extent, str(date) if date else None)


class Gateway:
    def __init__(self, profile, task_id, route: EffectiveRoute | None = None, evaluation_id=None, *, legacy_research=False):
        self.profile = profile
        self.task_id = task_id
        self.route = route
        self.evaluation_id = evaluation_id
        self.legacy_research = legacy_research
        self.key = secret(profile.id)
        config = settings()
        self.limits = Limits(**config["limits"])
        self.automatic_limit = config["research"]["automatic_daily_requests"]
        self.task = db.one("SELECT * FROM tasks WHERE id=?", (task_id,))
        self.search_checks = {}
        if self.task and self.task.get("request_limit"):
            self.limits.task_requests = self.task["request_limit"]

    def remaining(self):
        day = datetime.now(ZoneInfo(self.limits.timezone)).date().isoformat()
        daily = db.one("SELECT count(*) AS n FROM usage WHERE day=?", (day,))["n"]
        used = db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (self.task_id,))["n"]
        remaining = min(self.limits.task_requests - used, self.limits.daily_requests - daily)
        if self.task and self.task.get("automatic"):
            automatic = db.one("SELECT count(*) AS n FROM usage u JOIN tasks t ON t.id=u.task_id WHERE u.day=? AND t.automatic=1", (day,))["n"]
            remaining = min(remaining, self.automatic_limit - automatic)
        return max(0, remaining)

    def checkpoint(self, progress=None):
        task = db.one("SELECT cancel_requested,status FROM tasks WHERE id=?", (self.task_id,))
        if not task or task["cancel_requested"] or task["status"] != "running":
            raise Cancelled("任务已取消")
        if progress:
            db.execute("UPDATE tasks SET progress=?,heartbeat=? WHERE id=?", (progress, db.now(), self.task_id))

    def request_cost(self, payload, operation):
        if operation == "search":
            return self.profile.search_price
        # 用 UTF-8 字节数估算请求输入额度。
        size = len(json.dumps(payload, ensure_ascii=False).encode()) + 128
        output = payload.get("max_tokens") or payload.get("max_output_tokens") or payload.get("parameters", {}).get("max_tokens", 0)
        searches = payload.get("max_tool_calls", 1)
        return self.cost(operation, size, output, searches)

    def cost(self, operation, input_tokens, output_tokens, searches=1):
        search_price = self.profile.search_price
        if operation == "search":
            return search_price
        input_price, output_price = self.profile.input_price, self.profile.output_price
        if None in (input_tokens, output_tokens, input_price, output_price):
            return None
        generation = (input_tokens * input_price + output_tokens * output_price) / 1_000_000
        if operation == "search_generate":
            return None if search_price is None or searches is None else generation + search_price * searches
        return generation

    def reserve(self, operation, estimate=None, request_key=None):
        timestamp = datetime.now(timezone.utc)
        day = timestamp.astimezone(ZoneInfo(self.limits.timezone)).date().isoformat()
        with db.transaction() as connection:
            task = connection.execute("SELECT status,cancel_requested,automatic,priority FROM tasks WHERE id=?", (self.task_id,)).fetchone()
            if not task or task["status"] != "running" or task["cancel_requested"]:
                raise Cancelled("任务已取消")
            if task["automatic"] or task["priority"] <= 0:
                yield_to_manual(connection)
            daily = connection.execute("SELECT count(*) FROM usage WHERE day=?", (day,)).fetchone()[0]
            count = connection.execute("SELECT count(*) FROM usage WHERE task_id=?", (self.task_id,)).fetchone()[0]
            if daily >= self.limits.daily_requests:
                raise BudgetExceeded("已达到每日请求次数上限，请在设置中检查额度")
            if count >= self.limits.task_requests:
                raise BudgetExceeded("已达到单次任务请求次数上限")
            if self.task and self.task.get("automatic"):
                automatic = connection.execute("SELECT count(*) FROM usage u JOIN tasks t ON t.id=u.task_id WHERE u.day=? AND t.automatic=1", (day,)).fetchone()[0]
                if automatic >= self.automatic_limit:
                    raise BudgetExceeded("自动研究的每日请求额度已用尽，手动任务仍可使用剩余额度")
            for label, limit, spent in cost_windows(connection, self.limits, timestamp, day):
                if estimate is None:
                    raise BudgetExceeded("已启用费用上限，请先填写此模型的输入、输出或搜索单价")
                if spent + estimate > limit:
                    raise BudgetExceeded(f"已达到{label}费用上限")
            cursor = connection.execute(
                "INSERT INTO usage(task_id,profile_id,provider,model,operation,created_at,day,price_note,route,evaluation_id,reserved_estimate) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (self.task_id, self.profile.id, self.profile.provider, self.profile.model, operation, timestamp.isoformat(timespec="microseconds"), day, self.profile.price_note,
                 self.route.step if self.route else "", self.evaluation_id, estimate),
            )
            if request_key:
                connection.execute("UPDATE research_operations SET status='running',updated_at=? WHERE task_id=? AND operation_key=?", (db.now(), self.task_id, request_key))
            return cursor.lastrowid

    async def collect_stream(self, response):
        content, reasoning, calls = [], [], {}
        finish, usage, metadata = None, {}, {}
        ended = False
        checked = time.monotonic()
        async for event in EventSource(response).aiter_sse():
            if event.data == "[DONE]":
                ended = True
                break
            if not event.data:
                continue
            item = event.json()
            if item.get("error"):
                raise ResearchError("服务商流式请求返回错误")
            metadata.update({key: item[key] for key in ("id", "created", "model", "system_fingerprint") if key in item})
            if item.get("usage"):
                usage = item["usage"]
            for choice in item.get("choices", []):
                if choice.get("index", 0) != 0:
                    raise ResearchError("研究请求返回了未要求的多个候选结果")
                delta = choice.get("delta") or {}
                if delta.get("content"):
                    content.append(delta["content"])
                if delta.get("reasoning_content"):
                    reasoning.append(delta["reasoning_content"])
                for change in delta.get("tool_calls") or []:
                    target = calls.setdefault(change["index"], {"id": "", "type": "function", "function": {"name": "", "arguments": ""}})
                    if change.get("id"):
                        target["id"] = change["id"]
                    function = change.get("function") or {}
                    target["function"]["name"] += function.get("name") or ""
                    target["function"]["arguments"] += function.get("arguments") or ""
                if choice.get("finish_reason"):
                    finish = choice["finish_reason"]
            if time.monotonic() - checked >= 1:
                self.checkpoint()
                checked = time.monotonic()
        if not ended or not finish:
            raise UnknownOutcome("服务商数据流在完成标识之前中断，执行结果未知")
        message = {"role": "assistant", "content": "".join(content)}
        if reasoning:
            message["reasoning_content"] = "".join(reasoning)
        if calls:
            message["tool_calls"] = [calls[index] for index in sorted(calls)]
        return {**metadata, "choices": [{"index": 0, "message": message, "finish_reason": finish}], "usage": usage}

    async def post(self, path, payload, operation, request_key=None):
        started = time.monotonic()
        if request_key:
            digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False).encode()).hexdigest()
            previous = db.one("SELECT * FROM research_operations WHERE task_id=? AND operation_key=?", (self.task_id, request_key))
            if previous and previous["status"] == "success":
                saved_inputs = json.loads(previous["inputs_json"])
                if saved_inputs.get("request_hash") != digest or saved_inputs.get("path") != path:
                    raise ResearchError("保存的请求与当前内容或研究规则不同，请创建新任务；没有自动重复发送请求")
                return json.loads(previous["outputs_json"])
            if previous and previous["status"] in {"running", "unknown"}:
                raise UnknownOutcome("此前请求的执行结果未知，未自动重复收费请求；请在任务页面明确选择继续处理")
            if previous and previous["status"] in {"invalid", "truncated"}:
                raise ResearchError("已保存的模型返回内容未通过校验，请在任务页面选择继续处理；没有自动重复发送请求")
            db.execute("INSERT INTO research_operations(task_id,operation_key,kind,status,inputs_json,created_at,updated_at) VALUES(?,?,?,'retry_pending',?,?,?) ON CONFLICT(task_id,operation_key) DO UPDATE SET status='retry_pending',inputs_json=excluded.inputs_json,error='',updated_at=excluded.updated_at", (self.task_id, request_key, operation, json.dumps({"path": path, "request_hash": digest}), db.now(), db.now()))
        for attempt in range(3):
            self.checkpoint()
            try:
                # 后台任务退避后重试时重新检查手动队列和额度。
                usage_id = self.reserve(operation, self.request_cost(payload, operation), request_key)
            except BudgetExceeded:
                self.failed_operation(request_key, "blocked", "剩余调用额度不足")
                raise
            try:
                timeout = 60 if self.route and not self.route.step.startswith("deep_") else 600
                async with httpx.AsyncClient(timeout=httpx.Timeout(timeout, connect=15)) as client:
                    url = str(self.profile.base_url).rstrip("/") + path
                    headers = {"Authorization": f"Bearer {self.key}"}
                    if payload.get("stream"):
                        headers["Accept"] = "text/event-stream"
                        async with client.stream("POST", url, headers=headers, json=payload) as response:
                            if response.status_code < 400:
                                result = await self.collect_stream(response)
                            else:
                                await response.aread()
                                result = error_usage(response)
                    else:
                        response = await client.post(url, headers=headers, json=payload)
                        result = response.json() if response.status_code < 400 else error_usage(response)
                if response.status_code in (408, 429, 500, 502, 503, 504) and attempt < 2:
                    self.record_retry(usage_id, request_key, "retry", result, operation)
                    self.checkpoint("服务商暂时不可用，等待有限重试")
                    await asyncio.sleep(2 ** (attempt + 1))
                    continue
                if response.status_code >= 400:
                    labels = {401: "API 密钥无效", 403: "账户或模型没有访问权限", 402: "账户额度不足", 429: "服务商限流"}
                    raise ResearchError(f"{labels.get(response.status_code, '服务商拒绝请求')}（HTTP {response.status_code}）。请检查模型、地址和账户状态。")
                if result.get("error") or result.get("code"):
                    raise ResearchError("服务商返回错误，请检查配置和账户状态。错误响应内容未写入日志。")
                usage = result.get("usage") or {}
                input_tokens = usage.get("prompt_tokens", usage.get("input_tokens"))
                output_tokens = usage.get("completion_tokens", usage.get("output_tokens"))
                searches = 1
                if operation == "search_generate" and self.profile.provider == "volcengine":
                    searches = (usage.get("tool_usage") or {}).get("web_search")
                    if searches is None:
                        searches = payload["max_tool_calls"]
                estimate = self.cost(operation, input_tokens, output_tokens, searches)
                with db.transaction() as connection:
                    connection.execute("UPDATE usage SET status='success',input_tokens=?,output_tokens=?,estimate=? WHERE id=?", (input_tokens, output_tokens, estimate, usage_id))
                    if request_key:
                        connection.execute("UPDATE research_operations SET status='success',outputs_json=?,elapsed_ms=?,updated_at=? WHERE task_id=? AND operation_key=?", (json.dumps(result, ensure_ascii=False), int((time.monotonic() - started) * 1000), db.now(), self.task_id, request_key))
                return result
            except (httpx.ConnectError, httpx.ConnectTimeout):
                self.record_retry(usage_id, request_key, "network_error", {}, operation)
                if attempt == 2:
                    self.failed_operation(request_key, "failed", "无法连接服务商")
                    raise ResearchError("无法连接服务商，有限重试已结束，请检查本机网络和 API 地址") from None
                await asyncio.sleep(2 ** (attempt + 1))
            except (httpx.TransportError, SSEError) as error:
                db.execute("UPDATE usage SET status='unknown' WHERE id=?", (usage_id,))
                self.failed_operation(request_key, "unknown", "请求已发出，但未收到完整结果：" + type(error).__name__)
                raise UnknownOutcome(f"服务商请求已发出，但结果未知（{type(error).__name__}）；已保存资料，未自动重复请求") from None
            except asyncio.CancelledError:
                db.execute("UPDATE usage SET status='interrupted' WHERE id=?", (usage_id,))
                self.failed_operation(request_key, "unknown", "请求期间任务中断")
                raise
            except UnknownOutcome as error:
                db.execute("UPDATE usage SET status='unknown' WHERE id=?", (usage_id,))
                self.failed_operation(request_key, "unknown", str(error))
                raise
            except (ValueError, ResearchError):
                db.execute("UPDATE usage SET status='failed' WHERE id=?", (usage_id,))
                self.failed_operation(request_key, "failed", "请求未成功")
                raise
        raise ResearchError("请求未完成")

    def record_retry(self, usage_id, request_key, status, usage, operation):
        input_tokens = usage.get("prompt_tokens", usage.get("input_tokens"))
        output_tokens = usage.get("completion_tokens", usage.get("output_tokens"))
        estimate = self.cost(operation, input_tokens, output_tokens) if None not in (input_tokens, output_tokens) else None
        with db.transaction() as connection:
            connection.execute("UPDATE usage SET status=?,input_tokens=?,output_tokens=?,estimate=? WHERE id=?", (status, input_tokens, output_tokens, estimate, usage_id))
            if request_key:
                connection.execute("UPDATE research_operations SET status='retry_pending',updated_at=? WHERE task_id=? AND operation_key=?", (db.now(), self.task_id, request_key))

    def failed_operation(self, key, status, error):
        if key:
            db.execute("UPDATE research_operations SET status=?,error=?,updated_at=? WHERE task_id=? AND operation_key=?", (status, error, db.now(), self.task_id, key))

    @contextmanager
    def validate_result(self, key):
        try:
            yield
        except (ValueError, ResearchError) as error:
            self.failed_operation(key, "invalid", f"模型返回内容未通过校验（{type(error).__name__}）")
            raise

    async def search(self, query, request_key=None, reuse_query_key=None, reserve_requests=0):
        from .continuity import cached_operation, context_for_task, measurement_units, normalized
        if not self.task or self.task["kind"].startswith("test_"):
            return await self.search_uncached(query, request_key)
        job = json.loads(self.task.get("job_json") or "{}")
        async def produce(cache_key):
            if self.remaining() <= reserve_requests:
                raise BudgetExceeded("剩余调用已保留给整理与核验")
            sources = await self.search_uncached(query, request_key or "search:" + cache_key)
            return {"sources": [asdict(source) for source in sources]}
        inputs = {"query": normalized(query)}
        if measurement_units(query):
            inputs["units"] = measurement_units(query)
        result = await cached_operation("search_web", inputs, context_for_task(self.task), self.task_id, produce, force=job.get("force_refresh", False), reuse_key=reuse_query_key)
        self.search_checks[request_key or query] = {key: result[key] for key in ("cache_key", "reused", "checked_at", "source_task_id")}
        if result.get("reused_query"):
            self.search_checks[request_key or query]["reused_query"] = result["reused_query"]
        return [Source(**item) for item in result["sources"]]

    async def search_uncached(self, query, request_key=None):
        provider = self.profile.provider
        if provider == "deepseek":
            data = await self.post("/tools/search", {"query": query[:128], "count": 6, "need_content": True}, "search", request_key)
            return [normalized_source(item["title"], item["url"], item.get("content") or item.get("summary") or item.get("snippet", ""), "网页内容（服务商提取）" if item.get("content") else "搜索摘要", item.get("date")) for item in data["results"]]
        if provider == "zhipu":
            data = await self.post("/web_search", {"search_engine": "search_pro", "search_query": query, "count": 6, "content_size": "high"}, "search", request_key)
            return [normalized_source(item["title"], item["link"], item.get("content", ""), "搜索摘要", item.get("publish_date")) for item in data["search_result"]]
        if provider == "moonshot":
            data = await self.post("/tools/search_pro", {"text_query": query, "limit": 6, "timeout_seconds": 40}, "search", request_key)
            return [normalized_source(item["title"], item["url"], "\n".join(chunk["text"] for chunk in item.get("chunks", [])) or item.get("snippet", ""), "网页相关段落" if item.get("chunks") else "搜索摘要", item.get("date")) for item in data["search_results"]]
        raise ResearchError("当前服务商不支持独立搜索接口")

    async def complete(self, messages, search=False, max_tokens=None, structured=False, tools=None, reasoning_effort=None, request_key=None, operation=None, stream=None):
        provider = self.profile.provider
        ceiling = self.route.output_tokens if self.route else self.limits.output_tokens
        effort = self.route.reasoning_effort if self.route else reasoning_effort or ("disabled" if structured else self.profile.reasoning_effort)
        tokens = min(max_tokens or ceiling, ceiling)
        operation = operation or ("structured" if structured else "chat")
        message = {}
        if tools and provider in {"qwen", "volcengine"}:
            raise ResearchError("当前服务商适配器尚未验证原生工具调用，请选择已验证的研究配置")
        if provider == "qwen":
            payload = {"model": self.profile.model, "input": {"messages": messages}, "parameters": {"result_format": "message", "max_tokens": tokens}}
            if self.route and effort != "default":
                payload["parameters"]["enable_thinking"] = effort != "disabled"
            if structured:
                payload["parameters"]["response_format"] = {"type": "json_object"}
            if search:
                payload["parameters"].update(enable_search=True, search_options={"forced_search": True, "enable_source": True, "enable_citation": True, "citation_format": "[<number>]"})
            data = await self.post("/services/aigc/text-generation/generation", payload, "search_generate" if search else operation, request_key)
            output = data["output"]
            if output["choices"][0].get("finish_reason") == "length":
                raise ResearchError("回答达到输出 token 上限，未保存不完整成果；请提高上限或缩小问题范围")
            text = output["choices"][0]["message"]["content"]
            sources = [normalized_source(item["title"], item["url"], extent="服务商引用（未提供全文）") for item in output.get("search_info", {}).get("search_results", [])]
        elif provider == "volcengine" and search:
            payload = {"model": self.profile.model, "input": messages, "max_output_tokens": tokens,
                       "tools": [{"type": "web_search", "max_keyword": 1}], "max_tool_calls": ARK_SEARCH_ROUNDS}
            data = await self.post("/responses", payload, "search_generate", request_key)
            if data.get("status") == "incomplete":
                raise ResearchError("服务商返回了未完成的研究结果，请检查输出额度")
            parts, sources = [], []
            for item in data["output"]:
                if item.get("type") == "message":
                    for content in item.get("content", []):
                        if content.get("type") == "output_text":
                            parts.append(content["text"])
                            for annotation in content.get("annotations", []):
                                if annotation.get("type") == "url_citation":
                                    sources.append(normalized_source(annotation.get("title"), annotation["url"], extent="服务商引用（未提供全文）"))
                if item.get("type") == "web_search_call":
                    for source in (item.get("action") or {}).get("sources", []):
                        sources.append(normalized_source(source.get("title"), source["url"], extent="服务商引用（未提供全文）"))
            text = "\n".join(parts)
        else:
            if search:
                raise ResearchError("该接口不支持直接联网，请选择具备联网能力的配置")
            payload = {"model": self.profile.model, "messages": messages, "max_tokens": tokens, "stream": bool(request_key and not self.evaluation_id) if stream is None else stream}
            if payload["stream"]:
                payload["stream_options"] = {"include_usage": True}
            if tools:
                payload["tools"] = tools
            routed = self.route and not self.legacy_research
            if (provider == "deepseek" or routed and provider in {"zhipu", "moonshot", "volcengine"}) and effort != "default":
                payload["thinking"] = {"type": "disabled" if effort == "disabled" else "enabled"}
                if effort != "disabled" and provider in {"deepseek", "zhipu", "volcengine"}:
                    payload["reasoning_effort"] = effort
            if provider == "compatible" and routed and effort != "default":
                payload["reasoning_effort"] = "none" if effort == "disabled" else {"max": "xhigh"}.get(effort, effort)
            if structured and (not self.legacy_research or provider == "deepseek"):
                payload["response_format"] = {"type": "json_object"}
            data = await self.post("/chat/completions", payload, operation, request_key)
            choice = data["choices"][0]
            if choice.get("finish_reason") == "length":
                self.failed_operation(request_key, "truncated", "生成达到输出上限，原始响应已经保存")
                raise ResearchError("回答达到输出 token 上限，未保存不完整成果；请提高上限或缩小问题范围")
            text = choice["message"].get("content")
            message = choice["message"]
            sources = []
        if not (isinstance(text, str) and text.strip()) and not message.get("tool_calls"):
            raise ResearchError("服务商未返回有效正文，请检查所选模型和输出额度")
        return Reply(text or "", sources, message)
