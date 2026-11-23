import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TEST_DATA = ROOT / ".work" / "test-data"
os.environ["RESEARCHER_DATA_DIR"] = str(TEST_DATA)

from researcher import db
from researcher.app import app
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def clean_database():
    db.initialize()
    with db.transaction() as connection:
        connection.execute("PRAGMA defer_foreign_keys=ON")
        for table in ("evidence_processing", "knowledge_changes", "knowledge_facets", "research_cache", "source_checks", "source_aliases", "research_feedback", "knowledge_evidence", "knowledge_versions", "knowledge_items", "claim_evidence", "research_gaps", "research_experiences", "research_operations", "research_messages", "source_snapshots", "ingestion_queue", "document_chunks", "annotation_versions", "annotations", "judgment_evidence", "judgment_versions", "judgments", "event_topics", "event_entities", "event_claims", "event_aliases", "relations", "entity_fields", "coverage_checks", "daily_issues", "collection_runs", "task_steps", "research_questions", "watchlist_proposals", "claims", "document_versions", "document_task_links", "documents", "events", "sources", "briefs", "messages", "artifacts", "usage", "tasks", "research_sessions", "entities", "topics", "runtime"):
            connection.execute(f"DELETE FROM {table}")
        connection.execute("UPDATE station_settings SET watchlist_confirmed=0,sources_confirmed=0,auto_enabled=0,schedule_time='09:00',next_run=NULL,last_attempt_at=NULL,last_success_at=NULL,last_status='never',last_error='',last_visit_at=NULL,coverage_json='{}' WHERE id=1")
    for name in ("config.json", "secrets.json"):
        path = TEST_DATA / name
        if path.exists():
            path.unlink()


@pytest.fixture
def client():
    with TestClient(app, base_url="http://localhost") as session:
        session.get("/")
        yield session


@pytest.fixture
def csrf(client):
    return client.cookies["researcher_csrf"]


@pytest.fixture
def topic(client, csrf):
    response = client.post("/topics", data={"csrf": csrf, "title": "独立 AI 加速器领域概览", "kind": "领域", "question": "关注厂商产品、技术与软件进展、应用场景及市场竞争。"}, follow_redirects=False)
    assert response.status_code == 303
    return db.one("SELECT * FROM topics ORDER BY id DESC LIMIT 1")
