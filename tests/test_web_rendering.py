import secrets
from shutil import copytree
from urllib.parse import urlsplit

import pytest
from lxml import html

from researcher import db, research, station
from researcher.app import HERE, load_templates
from researcher.config import MODEL_STEPS, Profile, fingerprint, resolve_route, save_profile, settings, write_json
from researcher.ingest import benchmark
from researcher.providers import Gateway
from researcher.worker import claim_task


def settings_post(client, csrf, section, **values):
    response = client.post("/settings", data={"csrf": csrf, "section": section, **values})
    assert response.status_code == 200
    assert [item.status_code for item in response.history] == [303]
    assert response.template.name == "settings.html"
    assert response.context["notice"]
    assert not response.context["error"]
    return html.fromstring(response.content)


def test_running_renderer_keeps_its_templates(tmp_path):
    directory = tmp_path / "templates"
    copytree(HERE / "templates", directory)
    renderer = load_templates(directory)
    for name in ("settings.html", "base.html", "ui.html"):
        source = directory / name
        original = source.read_text()
        source.unlink()
        assert renderer.env.loader.get_source(renderer.env, name)[0] == original


@pytest.mark.parametrize(("path", "template"), [
    ("/", "home.html"), ("/research", "research.html"), ("/admin/eval", "admin_eval.html"),
    ("/admin/funnel", "admin_funnel.html"), ("/admin/sources", "admin_sources.html"),
    ("/evidence", "evidence.html"), ("/compare", "compare.html"),
])
def test_core_pages_render(client, path, template):
    response = client.get(path)
    assert response.status_code == 200
    assert response.template.name == template


def test_selectbench_displays_legacy_report_without_estimated_usage(client):
    samples = [row for row in benchmark.samples() if row["split"] == "development"]
    sample = next(row for row in samples if row["published_at"] is None and not row["expected"])
    result = {**sample, "score": None, "selected": False, "gate_reason": "missing_date"}
    report = benchmark.summarize(samples, [result], [])
    task = db.execute("INSERT INTO tasks(kind,status,profile_id,profile_fingerprint,provider,model,created_at,job_json,result_json) "
                      "VALUES('selectbench','success','','','','',?,?,?)",
                      (db.now(), station.encode({"split": "development"}), station.encode(report)))
    response = client.get("/admin/eval")
    assert response.status_code == 200
    document = html.fromstring(response.content)
    assert document.xpath("//a[@href=$path]", path=f"/tasks/{task}")
    assert document.xpath("//th[normalize-space()='历史评测']")
    assert document.xpath("//td[normalize-space()='¥ 0']")


@pytest.mark.parametrize("legacy", [False, True])
def test_settings_renders_default_and_legacy_config(client, csrf, legacy):
    if legacy:
        write_json("config.json", {
            "profiles": [{"id": "glm", "name": "智谱", "provider": "zhipu", "model": "GLM-5.2",
                          "base_url": "https://open.bigmodel.cn/api/paas/v4"}],
            "active_profile": "glm", "limits": {"daily_requests": 100, "minute_cost": 2.2, "hour_cost": 4.5, "daily_cost": 6.5},
            "model_routes": {"write": {"output_tokens": 2048}, "deep_plan": {"profile_id": "glm"}},
            "research": {"planning_effort": "max", "planning_output_tokens": 3072},
        })
    response = client.get("/settings")
    assert response.status_code == 200
    assert response.template.name == "settings.html"
    document = html.fromstring(response.content)
    assert document.xpath("//input[@name='cost_profile' and @checked]/@value") == ["balanced"]
    assert document.xpath("//input[@name='daily_cost']/@value") == (["6.5"] if legacy else [""])
    assert document.xpath("//input[@name='monthly_cost']/@value") == [""]
    assert {item["step"] for item in response.context["routes"]} == set(MODEL_STEPS)
    if not legacy:
        assert "尚未配置模型" in response.text
        return

    assert document.xpath("//select[@name='tier']/option[@selected]/@value") == ["primary", "primary"]
    assert document.xpath("//input[@name='cluster_output_tokens']/@value") == ["2048"]
    assert document.xpath("//select[@name='deep_write_profile_id']/option[@selected]/@value") == ["glm"]
    profile = resolve_route("deep_plan").profile
    write_json("secrets.json", {profile.id: secrets.token_urlsafe()})
    task = {"job_json": "{}", "profile_id": profile.id, "profile_fingerprint": fingerprint(profile)}
    settings_post(client, csrf, "cost", cost_profile="eco")
    plan, _, _ = research.check_routes(task)
    assert (plan.reasoning_effort, plan.output_tokens) == ("max", 3072)
    assert settings()["limits"]["daily_requests"] == 100


def test_setting_alias_preserves_query(client):
    response = client.get("/setting?notice=ready")
    assert response.status_code == 200
    assert [item.status_code for item in response.history] == [308]
    assert response.url.path == "/settings" and response.context["notice"] == "ready"


@pytest.mark.parametrize("preset", ["eco", "balanced", "deep"])
def test_settings_switches_preset_and_renders_selection(client, csrf, preset):
    document = settings_post(client, csrf, "cost", cost_profile=preset)
    assert document.xpath("//input[@name='cost_profile' and @checked]/@value") == [preset]
    assert settings()["cost_profile"] == preset


def test_settings_cost_limits_clear_and_allow_unpriced_research(client, csrf):
    profile = Profile(id="glm", name="智谱", provider="zhipu", model="GLM-5.2", chat_verified=db.now(),
                      base_url="https://open.bigmodel.cn/api/paas/v4")
    save_profile(profile, secrets.token_urlsafe())
    document = settings_post(client, csrf, "limits", daily_cost="6.5", monthly_cost="35")
    assert document.xpath("//input[@name='daily_cost']/@value") == ["6.5"]
    assert document.xpath("//input[@name='monthly_cost']/@value") == ["35.0"]
    document = settings_post(client, csrf, "limits", daily_cost="", monthly_cost=" ")
    assert document.xpath("//input[@name='daily_cost' or @name='monthly_cost']/@value") == ["", ""]
    assert settings()["limits"]["daily_cost"] is settings()["limits"]["monthly_cost"] is None

    response = client.post("/research", data={"csrf": csrf, "message": "核对 Hailo-10H 的公开部署条件"})
    assert response.status_code == 200 and response.template.name == "research_session.html"
    task = claim_task(True)
    assert task and task["session_id"] == response.context["session"]["id"]
    plan, _, _ = research.check_routes(task)
    gateway = Gateway(plan.profile, task["id"], plan)
    assert gateway.reached_cost_redline() is None
    usage_id = gateway.reserve("plan", estimate=None)
    assert db.one("SELECT status,estimated_cost FROM model_call_ledger WHERE id=?", (usage_id,)) == {
        "status": "reserved", "estimated_cost": None,
    }


def test_settings_saves_slot_override_and_renders_validation_notice(client, csrf):
    profile = Profile(id="glm", name="智谱", provider="zhipu", model="GLM-5.2", base_url="https://open.bigmodel.cn/api/paas/v4")
    save_profile(profile, secrets.token_urlsafe())
    document = settings_post(client, csrf, "routes", override_switches="yes", deep_write_enabled="yes",
                             deep_write_profile_id="glm", deep_write_reasoning_effort="high", deep_write_output_tokens="3072")
    assert document.xpath("//input[@name='deep_write_enabled' and @checked]/@value") == ["yes"]
    assert document.xpath("//select[@name='deep_write_profile_id']/option[@selected]/@value") == ["glm"]
    route = resolve_route("deep_write")
    assert (route.profile.id, route.reasoning_effort, route.output_tokens) == ("glm", "high", 3072)

    response = client.post("/settings", data={"csrf": csrf, "section": "routes", "deep_write_output_tokens": "0"})
    assert response.status_code == 200
    location = urlsplit(response.history[0].headers["location"])
    assert location.fragment == "routes" and response.context["error"]
    assert resolve_route("deep_write") == route
