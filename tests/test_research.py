from researcher import db
from researcher.config import settings
import asyncio

import pytest

from researcher.knowledge import public_url
from researcher.providers import ResearchError


def test_research_entry_and_csrf(client, csrf):
    response = client.get("/research")
    assert response.status_code == 200
    assert "提出研究目标" in response.text
    assert client.post("/research", data={"message": "研究已有公开资料中的部署条件"}).status_code == 403
    response = client.post("/research", data={"csrf": csrf, "message": "研究已有公开资料中的部署条件"})
    assert "请在设置中添加并选择模型配置" in response.text
    assert not db.query("SELECT * FROM research_sessions")


def test_research_budget_and_settings_persist(client, csrf):
    response = client.post("/settings/research", data={"csrf": csrf, "question_requests": 18, "automatic_daily_requests": 20, "device": "cpu"})
    assert response.status_code == 200
    assert settings()["research"]["question_requests"] == 18
    assert settings()["research"]["device"] == "cpu"
    client.post("/settings/limits", data={"csrf": csrf, "daily_requests": 60, "task_requests": 8, "output_tokens": 4096})
    assert settings()["research"]["question_requests"] == 18
    client.post("/settings/research", data={"csrf": csrf, "question_requests": 0})
    assert settings()["research"]["question_requests"] == 18


def test_empty_research_session_and_evidence_are_not_created(client, csrf):
    assert client.get("/research/999999").status_code == 404
    assert client.post("/research/999999/messages", data={"csrf": csrf, "message": "继续检查原文"}).status_code == 404
    assert db.one("SELECT count(*) AS n FROM claim_evidence")["n"] == 0
    assert db.one("SELECT count(*) AS n FROM knowledge_items")["n"] == 0


@pytest.mark.parametrize("url", ["http://127.0.0.1", "http://169.254.169.254", "http://[::1]", "http://localhost"])
def test_source_reader_rejects_private_network(url):
    with pytest.raises(ResearchError):
        asyncio.run(public_url(url))
