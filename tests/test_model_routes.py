import asyncio
import hashlib
import json
import secrets

import pytest

from researcher import db
from researcher.config import COST_PROFILES, MODEL_STEPS, Profile, resolve_route, save_profile, settings, write_json
from researcher.providers import BudgetExceeded, Gateway


@pytest.mark.parametrize(("name", "rounds", "pages", "tiers", "polish", "plan"), [
    ("eco", 1, 4, ("economy",) * 6, False, "glm"),
    ("balanced", 2, 10, ("economy", "economy", "economy", "primary", "primary", "primary"), False, "glm"),
    ("deep", 3, 20, ("economy", "primary", "primary", "reasoning", "primary", "reasoning"), True, "deepseek"),
])
def test_presets_select_verified_models(client, csrf, name, rounds, pages, tiers, polish, plan):
    assert settings()["cost_profile"] == "balanced"
    profiles = (
        Profile(id="glm", name="智谱", provider="zhipu", base_url="https://open.bigmodel.cn/api/paas/v4",
                model="GLM-5.2", tier="primary", chat_verified=db.now()),
        Profile(id="qwen", name="通义千问", provider="qwen", base_url="https://dashscope.aliyuncs.com/api/v1",
                model="qwen-turbo", tier="economy", chat_verified=db.now()),
        Profile(id="deepseek", name="DeepSeek", provider="deepseek", base_url="https://api.deepseek.com/v1",
                model="deepseek-reasoner", tier="reasoning", chat_verified=db.now()),
    )
    for profile in profiles:
        save_profile(profile, secrets.token_urlsafe())
    response = client.post("/settings/cost", data={"csrf": csrf, "cost_profile": name}, follow_redirects=False)
    assert response.status_code == 303 and settings()["cost_profile"] == name
    policy = COST_PROFILES[name]
    assert (policy.review_rounds, policy.web_pages, policy.polish_briefing) == (rounds, pages, polish)
    assert tuple(policy.slots[step][0] for step in MODEL_STEPS) == tiers
    assert tuple(resolve_route(step).profile.id for step in MODEL_STEPS) == (
        "qwen", "glm" if name == "deep" else "qwen", "glm" if name == "deep" else "qwen",
        plan, "qwen" if name == "eco" else "glm", plan,
    )


def test_route_fallback_and_step_override():
    deep = Profile(id="deepseek", name="DeepSeek", provider="deepseek", base_url="https://api.deepseek.com/v1", model="deepseek-chat")
    fast = Profile(id="qwen", name="通义千问", provider="qwen", base_url="https://dashscope.aliyuncs.com/api/v1", model="qwen-plus")
    config = settings()
    config.update(active_profile=deep.id, profiles=[profile.model_dump(mode="json") for profile in (deep, fast)])
    config["model_routes"]["score"] = {"profile_id": fast.id, "output_tokens": 768}
    config["model_routes"]["deep_plan"] = {"reasoning_effort": "max"}
    config["model_routes"]["deep_write"] = {"profile_id": fast.id}
    config["model_routes"]["deep_review"] = {"enabled": False, "profile_id": fast.id}
    write_json("config.json", config)
    assert set(settings()["model_routes"]) == set(MODEL_STEPS)
    assert resolve_route("score").profile.id == fast.id
    assert resolve_route("score").output_tokens == 768
    assert resolve_route("score").reasoning_effort == "disabled"
    assert resolve_route("deep_plan").profile.id == deep.id
    assert resolve_route("deep_plan").reasoning_effort == "max"
    assert resolve_route("deep_write").profile.id == fast.id
    assert resolve_route("deep_review").profile.id == deep.id
    assert resolve_route("deep_review").output_tokens == 8192


@pytest.mark.parametrize(("field", "label"), [("daily_cost", "每日"), ("monthly_cost", "每月")])
def test_budget_reservations_are_shared_across_routes(field, label):
    profile = Profile(id="deepseek", name="DeepSeek", provider="deepseek", base_url="https://api.deepseek.com/v1", model="deepseek-chat")
    save_profile(profile, secrets.token_urlsafe())
    config = settings()
    config["limits"][field] = 0.01
    write_json("config.json", config)
    task = db.execute("INSERT INTO tasks(kind,status,profile_id,profile_fingerprint,provider,model,created_at) VALUES('feed','running',?,?,?,?,?)", (profile.id, "", profile.provider, profile.model, db.now()))
    Gateway(profile, task, resolve_route("prefilter")).reserve("prefilter", 0.007)
    with pytest.raises(BudgetExceeded, match=label) as error:
        Gateway(profile, task, resolve_route("score")).reserve("score", 0.007)
    assert all(value in str(error.value) for value in ("单次预计费用 ¥0.007", "窗口内已用 ¥0.007", "配置上限 ¥0.01"))
    assert db.query("SELECT route,reserved_estimate FROM usage") == [{"route": "prefilter", "reserved_estimate": 0.007}]


def test_deep_reservation_estimates_multilingual_tokens_and_reports_missing_prices():
    profile = Profile(id="glm", name="智谱", provider="zhipu", model="GLM-5.2", base_url="https://open.bigmodel.cn/api/paas/v4",
                      input_price=8, output_price=28)
    save_profile(profile, secrets.token_urlsafe())
    config = settings()
    config["limits"]["daily_cost"] = 0.10
    write_json("config.json", config)
    task = db.execute("INSERT INTO tasks(kind,status,profile_id,profile_fingerprint,provider,model,created_at) VALUES('interactive','running',?,?,?,?,?)",
                      (profile.id, "", profile.provider, profile.model, db.now()))
    review = Gateway(profile, task, resolve_route("deep_review"))
    payload = {"messages": [{"role": "user", "content": "核对爱芯元智与瑞芯微的量化和吞吐测试条件。" * 50}], "max_tokens": 32768}
    assert review.request_cost(payload, "structured") > 0.90
    payload["max_tokens"] = 2048
    estimate = review.request_cost(payload, "structured")
    assert 0 < estimate < 0.10
    with pytest.raises(BudgetExceeded, match="GLM-5.2.*deep_review.*搜索单价"):
        review.reserve("search", review.request_cost({}, "search"))
    with pytest.raises(BudgetExceeded, match="GLM-5.2.*deep_review.*输出单价"):
        Gateway(profile.model_copy(update={"output_price": None}), task, review.route).reserve("structured")
    with pytest.raises(BudgetExceeded, match=r"每日总费用安全红线.*单次预计费用 ¥0.2.*配置上限 ¥0.1"):
        review.reserve("structured", 0.2)
    assert not db.query("SELECT id FROM usage")
    review.reserve("structured", estimate)
    assert db.one("SELECT reserved_estimate FROM usage")["reserved_estimate"] == estimate


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
