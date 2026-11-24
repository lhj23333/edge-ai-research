import asyncio
import hashlib
import json
import secrets

import pytest

from researcher import db
from researcher.config import MODEL_STEPS, Profile, resolve_route, save_profile, settings, write_json
from researcher.providers import BudgetExceeded, Gateway


def test_route_fallback_and_step_override():
    deep = Profile(id="deepseek", name="DeepSeek", provider="deepseek", base_url="https://api.deepseek.com/v1", model="deepseek-chat")
    fast = Profile(id="qwen", name="通义千问", provider="qwen", base_url="https://dashscope.aliyuncs.com/api/v1", model="qwen-plus")
    config = settings()
    config.update(active_profile=deep.id, profiles=[profile.model_dump(mode="json") for profile in (deep, fast)])
    config["model_routes"]["score"] = {"profile_id": fast.id, "output_tokens": 768}
    config["research"]["planning_effort"] = "max"
    write_json("config.json", config)
    assert set(settings()["model_routes"]) == set(MODEL_STEPS)
    assert resolve_route("score").profile.id == fast.id
    assert resolve_route("score").output_tokens == 768
    assert resolve_route("score").reasoning_effort == "disabled"
    assert resolve_route("deep_plan").profile.id == deep.id
    assert resolve_route("deep_plan").reasoning_effort == "max"
    assert resolve_route("deep_review").output_tokens == 32768


@pytest.mark.parametrize(("field", "label"), [("minute_cost", "每分钟"), ("hour_cost", "每小时"), ("daily_cost", "每日")])
def test_budget_reservations_are_shared_across_routes(field, label):
    profile = Profile(id="deepseek", name="DeepSeek", provider="deepseek", base_url="https://api.deepseek.com/v1", model="deepseek-chat")
    save_profile(profile, secrets.token_urlsafe())
    config = settings()
    config["limits"][field] = 0.01
    write_json("config.json", config)
    task = db.execute("INSERT INTO tasks(kind,status,profile_id,profile_fingerprint,provider,model,created_at) VALUES('feed','running',?,?,?,?,?)", (profile.id, "", profile.provider, profile.model, db.now()))
    Gateway(profile, task, resolve_route("prefilter")).reserve("prefilter", 0.007)
    with pytest.raises(BudgetExceeded, match=label):
        Gateway(profile, task, resolve_route("score")).reserve("score", 0.007)
    assert db.query("SELECT route,reserved_estimate FROM usage") == [{"route": "prefilter", "reserved_estimate": 0.007}]


@pytest.mark.parametrize(("provider", "model", "url", "searches"), [
    ("qwen", "qwen-plus", "https://dashscope.aliyuncs.com/api/v1", 1),
    ("volcengine", "doubao-seed-2-0-pro", "https://ark.cn-beijing.volces.com/api/v3", 3),
])
def test_search_generation_reserves_both_charges(provider, model, url, searches):
    profile = Profile(id=provider, name=provider, provider=provider, base_url=url, model=model,
                      input_price=2, output_price=10, search_price=0.01)
    save_profile(profile, secrets.token_urlsafe())
    config = settings()
    config["limits"]["daily_cost"] = 0.01 * searches + 0.001
    write_json("config.json", config)
    task = db.execute("INSERT INTO tasks(kind,status,profile_id,profile_fingerprint,provider,model,created_at) VALUES('test_search','running',?,?,?,?,?)",
                      (profile.id, "", provider, model, db.now()))
    gateway = Gateway(profile, task)
    messages = [{"role": "user", "content": "查找 Hailo-10H 的官方产品说明"}]
    with pytest.raises(BudgetExceeded, match="每日"):
        asyncio.run(gateway.complete(messages, search=True, max_tokens=512, request_key="search-test"))
    assert db.one("SELECT kind,status FROM research_operations") == {"kind": "search_generate", "status": "blocked"}
    assert not db.query("SELECT id FROM usage")
    assert gateway.cost("search", None, None) == 0.01
    assert gateway.cost("search_generate", 400, 300, searches) == pytest.approx(0.0038 + 0.01 * searches)


@pytest.mark.parametrize(("effort", "thinking"), [("default", {}), ("disabled", {"enable_thinking": False}), ("low", {"enable_thinking": True})])
def test_qwen_route_uses_the_explicit_thinking_setting_when_present(effort, thinking):
    profile = Profile(id="qwen", name="通义千问", provider="qwen", base_url="https://dashscope.aliyuncs.com/api/v1",
                      model="qwen-plus", input_price=2, output_price=10)
    save_profile(profile, secrets.token_urlsafe())
    config = settings()
    config["limits"]["daily_cost"] = 0.001
    config["model_routes"]["prefilter"] = {"reasoning_effort": effort}
    write_json("config.json", config)
    task = db.execute("INSERT INTO tasks(kind,status,profile_id,profile_fingerprint,provider,model,created_at) VALUES('feed','running',?,?,?,?,?)",
                      (profile.id, "", profile.provider, profile.model, db.now()))
    gateway = Gateway(profile, task, resolve_route("prefilter"))
    messages = [{"role": "user", "content": "概括 Hailo-10H 的产品用途"}]
    with pytest.raises(BudgetExceeded, match="每日"):
        asyncio.run(gateway.complete(messages, request_key="thinking-test"))
    parameters = {"result_format": "message", "max_tokens": 512, **thinking}
    expected = {"model": profile.model, "input": {"messages": messages}, "parameters": parameters}
    request = json.loads(db.one("SELECT inputs_json FROM research_operations")["inputs_json"])
    assert request["request_hash"] == hashlib.sha256(json.dumps(expected, ensure_ascii=False).encode()).hexdigest()
