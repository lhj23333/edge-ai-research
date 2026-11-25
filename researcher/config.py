import fcntl
import hashlib
import json
import os
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, HttpUrl, field_validator

ROOT = Path(__file__).resolve().parent.parent
DATA = Path(os.environ.get("RESEARCHER_DATA_DIR", ROOT / "data")).resolve()

PRESETS = {
    "qwen": {"name": "通义千问", "base_url": "https://dashscope.aliyuncs.com/api/v1", "model": "qwen-plus", "search": "DashScope 联网与来源信息"},
    "deepseek": {"name": "DeepSeek", "base_url": "https://api.deepseek.com/v1", "model": "", "search": "Search API + 对话"},
    "zhipu": {"name": "智谱", "base_url": "https://open.bigmodel.cn/api/paas/v4", "model": "", "search": "Web Search API + 对话"},
    "volcengine": {"name": "火山方舟", "base_url": "https://ark.cn-beijing.volces.com/api/v3", "model": "", "search": "Responses 联网工具"},
    "moonshot": {"name": "Moonshot", "base_url": "https://api.moonshot.cn/v1", "model": "", "search": "Web Search Pro + 对话"},
    "compatible": {"name": "OpenAI 兼容", "base_url": "", "model": "", "search": "仅基于已有资料讨论"},
}

Effort = Literal["low", "high", "max", "disabled", "default"]
CostProfile = Literal["eco", "balanced", "deep"]
ModelTier = Literal["economy", "primary", "reasoning"]
MODEL_STEPS = ("prefilter", "score", "cluster", "deep_plan", "deep_write", "deep_review")
STEP_LABELS = dict(zip(MODEL_STEPS, ("条目预筛", "条目评分", "摘要与聚簇", "深研规划", "深研撰写", "深研审阅"), strict=True))
MODEL_TIERS = {"economy": "廉价模型", "primary": "主力模型", "reasoning": "推理模型"}


@dataclass(frozen=True)
class CostPolicy:
    label: str
    web_pages: int
    review_rounds: int
    event_candidates: int
    daily_topic_calls: int
    polish_briefing: bool
    slots: Mapping[str, tuple[ModelTier, Effort, int]]


COST_PROFILES = {
    "eco": CostPolicy("极省", 4, 1, 4, 8, False, dict(zip(MODEL_STEPS, (
        ("economy", "disabled", 384), ("economy", "disabled", 512), ("economy", "disabled", 1536),
        ("economy", "disabled", 2048), ("economy", "disabled", 4096), ("economy", "disabled", 4096),
    ), strict=True))),
    "balanced": CostPolicy("均衡", 10, 2, 8, 16, False, dict(zip(MODEL_STEPS, (
        ("economy", "disabled", 512), ("economy", "disabled", 768), ("economy", "disabled", 1536),
        ("primary", "low", 4096), ("primary", "disabled", 8192), ("primary", "low", 8192),
    ), strict=True))),
    "deep": CostPolicy("深度", 20, 3, 12, 24, True, dict(zip(MODEL_STEPS, (
        ("economy", "disabled", 512), ("primary", "disabled", 1024), ("primary", "low", 2048),
        ("reasoning", "high", 8192), ("primary", "low", 12288), ("reasoning", "high", 16384),
    ), strict=True))),
}


class ModelRoute(BaseModel):
    enabled: bool | None = None
    profile_id: str | None = None
    reasoning_effort: Effort | None = None
    output_tokens: int | None = Field(default=None, ge=128, le=32768)


def model_routes(values):
    if set(values) - set(MODEL_STEPS):
        raise ValueError("包含未知的模型步骤")
    return {name: ModelRoute.model_validate(values.get(name, {})).model_dump(exclude_none=True) for name in MODEL_STEPS}


class Limits(BaseModel):
    daily_requests: int = Field(default=60, ge=1, le=10000)
    task_requests: int = Field(default=8, ge=2, le=100)
    output_tokens: int = Field(default=4096, ge=256, le=32768)
    timezone: str = "Asia/Shanghai"
    daily_cost: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    monthly_cost: float | None = Field(default=None, gt=0, allow_inf_nan=False)

    @field_validator("timezone")
    @classmethod
    def valid_timezone(cls, value):
        if not value.strip():
            raise ValueError("时区不能为空，请填写有效的 IANA 时区名称")
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ValueError("时区无效，请填写有效的 IANA 时区名称") from error
        return value


class ResearchSettings(BaseModel):
    query_cache_hours: int = Field(default=6, ge=1, le=24)
    question_requests: int = Field(default=24, ge=4, le=100)
    revise_requests: int = Field(default=12, ge=3, le=100)
    onboard_requests: int = Field(default=8, ge=4, le=100)
    followup_requests: int = Field(default=12, ge=4, le=100)
    automatic_daily_requests: int = Field(default=36, ge=0, le=10000)
    ingest_requests: int = Field(default=24, ge=4, le=100)
    automatic_event_research: bool = False
    followups_per_day: int = Field(default=2, ge=0, le=20)
    max_rounds: int = Field(default=8, ge=1, le=30)
    timeout_minutes: int = Field(default=30, ge=1, le=120)
    context_chunks: int = Field(default=16, ge=4, le=40)
    max_source_bytes: int = Field(default=20_000_000, ge=100000, le=100_000_000)
    source_fresh_days: int = Field(default=7, ge=1, le=90)
    device: str = Field(default="cuda:0", pattern=r"^(cpu|cuda:[0-9]+)$")
    embedding_model: str = "BAAI/bge-m3"
    reranker_model: str = "BAAI/bge-reranker-v2-m3"


LEGACY_RESEARCH_PARAMETERS = {
    "planning_effort": "high", "planning_output_tokens": 8192,
    "review_effort": "high", "review_output_tokens": 32768,
    "synthesis_effort": "disabled", "synthesis_output_tokens": 8192,
}


def legacy_research_parameters(config):
    return {**LEGACY_RESEARCH_PARAMETERS, **config["legacy_research"]}


class CostSettings(BaseModel):
    cost_profile: CostProfile = "balanced"
    briefing_polish: bool | None = None


class Profile(BaseModel):
    id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,60}$")
    name: str = Field(min_length=1, max_length=80)
    provider: str
    base_url: HttpUrl
    model: str = Field(min_length=1, max_length=120)
    tier: ModelTier = "primary"
    reasoning_effort: Effort = "low"
    input_price: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    output_price: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    search_price: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    price_note: str = Field(default="", max_length=300)
    chat_verified: str | None = None
    search_verified: str | None = None
    tools_verified: str | None = None

    @field_validator("provider")
    @classmethod
    def valid_provider(cls, value):
        if value not in PRESETS:
            raise ValueError("不支持的服务商")
        return value

    @field_validator("base_url")
    @classmethod
    def secure_url(cls, value):
        if value.scheme != "https" or value.username or value.password or value.query or value.fragment:
            raise ValueError("API 地址必须为不含凭据、查询或片段的 HTTPS 地址")
        return value


def prepare_data():
    DATA.mkdir(parents=True, exist_ok=True, mode=0o700)
    DATA.chmod(0o700)


@contextmanager
def config_lock():
    prepare_data()
    with (DATA / "config.lock").open("a") as lock:
        os.chmod(lock.name, 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def read_json(name, default):
    path = DATA / name
    return json.loads(path.read_text()) if path.exists() else default


def write_json(name, value):
    path = DATA / name
    temporary = path.with_suffix(".new")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)
    path.chmod(0o600)


def stored_routes(routes):
    routes = {} if routes is None else routes
    migrated = {step: routes[step] if routes.get(step) is not None else {} for step in MODEL_STEPS}
    if not migrated["cluster"]:
        migrated["cluster"] = routes.get("write") or routes.get("cluster_review") or {}
    if "deep_write" not in routes and routes.get("deep_plan"):
        migrated["deep_write"] = routes["deep_plan"]
    return model_routes(migrated)


def settings():
    data = read_json("config.json", {})
    items = []
    for raw in data.get("profiles", []):
        profile = Profile(**raw)
        if profile.provider == "deepseek" and "reasoning_effort" not in raw:
            profile.chat_verified = None
            profile.search_verified = None
        items.append(profile.model_dump(mode="json"))
    research = data.get("research", {})
    old_parameters = {**research, **data.get("legacy_research", {})}
    return {**CostSettings.model_validate(data).model_dump(), "profiles": items, "active_profile": data.get("active_profile", ""),
            "limits": Limits(**data.get("limits", {})).model_dump(), "research": ResearchSettings(**research).model_dump(),
            "model_routes": stored_routes(data.get("model_routes", {})),
            "legacy_research": {key: old_parameters[key] for key in LEGACY_RESEARCH_PARAMETERS if key in old_parameters}}


def get_profile(profile_id=None, config=None):
    config = config if config is not None else settings()
    profile_id = profile_id or config["active_profile"]
    for item in config["profiles"]:
        if item["id"] == profile_id:
            return Profile(**item)
    raise ValueError("请在设置中添加并选择模型配置")


@dataclass(frozen=True)
class EffectiveRoute:
    step: str
    profile: Profile
    reasoning_effort: Effort
    output_tokens: int


def resolve_route(step, *, config=None):
    config = config if config is not None else settings()
    route = config["model_routes"][step]
    if route.get("enabled") is False:
        route = {}
    tier, effort, tokens = COST_PROFILES[config["cost_profile"]].slots[step]
    selected = route.get("profile_id")
    if not selected:
        compatible = [item for item in config["profiles"] if item["chat_verified"]
                      and (step not in {"deep_plan", "deep_review"} or item["provider"] not in {"qwen", "volcengine"})]
        eligible = [item for item in compatible if item.get("tier", "primary") == tier]
        if not eligible and step.startswith("deep_"):
            eligible = compatible
        if eligible and all(item["id"] != config["active_profile"] for item in eligible):
            selected = eligible[0]["id"]
    return EffectiveRoute(step, get_profile(selected, config),
                          route.get("reasoning_effort", effort), route.get("output_tokens", tokens))


def describe_routes(config):
    for step, (tier, effort, tokens) in COST_PROFILES[config["cost_profile"]].slots.items():
        yield {"step": step, "values": config["model_routes"][step], "tier": MODEL_TIERS[tier], "effort": effort, "tokens": tokens}


def secret(profile_id):
    key = read_json("secrets.json", {}).get(profile_id, "")
    if not key:
        raise ValueError("该配置尚未保存 API 密钥")
    return key


def fingerprint(profile):
    values = [profile.provider, str(profile.base_url), profile.model, profile.reasoning_effort, secret(profile.id)]
    return hashlib.sha256(json.dumps(values).encode()).hexdigest()


def research_routes(config=None):
    config = config if config is not None else settings()
    return tuple(resolve_route(step, config=config) for step in ("deep_plan", "deep_review", "deep_write"))


def routes_fingerprint(routes):
    values = [(route.step, route.profile.id, fingerprint(route.profile), route.reasoning_effort, route.output_tokens) for route in routes]
    return hashlib.sha256(json.dumps(values).encode()).hexdigest()


def save_profile(profile, api_key):
    with config_lock():
        config = settings()
        keys = read_json("secrets.json", {})
        previous = next((item for item in config["profiles"] if item["id"] == profile.id), None)
        if not api_key and profile.id not in keys:
            raise ValueError("首次保存配置时需要填写 API 密钥")
        changed = not previous or any(str(previous.get(field)) != str(getattr(profile, field)) for field in ("provider", "base_url", "model", "reasoning_effort"))
        if not changed and not api_key:
            profile.chat_verified = previous.get("chat_verified")
            profile.search_verified = previous.get("search_verified")
            profile.tools_verified = previous.get("tools_verified")
        if api_key:
            keys[profile.id] = api_key
        config["profiles"] = [item for item in config["profiles"] if item["id"] != profile.id] + [profile.model_dump(mode="json")]
        if not config["active_profile"]:
            config["active_profile"] = profile.id
        write_json("secrets.json", keys)
        write_json("config.json", config)


def mark_verified(profile, mode, timestamp, expected_fingerprint):
    with config_lock():
        current = get_profile(profile.id)
        if fingerprint(current) != expected_fingerprint:
            raise ValueError("测试过程中模型配置发生变化，请重新测试")
        config = settings()
        for item in config["profiles"]:
            if item["id"] == profile.id:
                item[f"{mode}_verified"] = timestamp
        write_json("config.json", config)
