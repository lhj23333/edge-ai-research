import os
import tempfile
import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from lxml import etree

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / ".work"
WORK.mkdir(exist_ok=True)
TEST_DIRECTORY = tempfile.TemporaryDirectory(prefix="suite-", dir=WORK)
TEST_DATA = Path(TEST_DIRECTORY.name)
os.environ["RESEARCHER_DATA_DIR"] = str(TEST_DATA)

from researcher import db
from researcher.app import app
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def clean_database():
    db.initialize()
    with db.transaction() as connection:
        connection.execute("PRAGMA defer_foreign_keys=ON")
        connection.execute("DELETE FROM feed_source_lifecycle")
        for table in ("feed_manual_reviews", "document_fetch_failures", "entity_aliases", "entity_claims",
                      "topic_retry_quotas", "content_cache", "research_evidence_versions"):
            connection.execute(f"DELETE FROM {table}")
        for table in ("daily_briefings", "event_cluster_decisions", "item_evaluations", "feed_observations", "feed_items", "feed_runs", "feed_sources", "evidence_processing", "knowledge_changes", "knowledge_facets", "research_cache", "source_checks", "source_aliases", "research_feedback", "knowledge_evidence", "knowledge_versions", "knowledge_items", "claim_evidence", "research_gaps", "research_experiences", "research_operations", "research_messages", "source_snapshots", "ingestion_queue", "document_chunks", "annotation_versions", "annotations", "judgment_evidence", "judgment_versions", "judgments", "event_topics", "event_entities", "event_claims", "event_aliases", "relations", "entity_fields", "coverage_checks", "daily_issues", "collection_runs", "task_steps", "research_questions", "watchlist_proposals", "claims", "document_versions", "document_task_links", "documents", "events", "briefs", "messages", "artifacts", "usage", "tasks", "research_sessions", "entities", "topics", "runtime"):
            connection.execute(f"DELETE FROM {table}")
        connection.execute("UPDATE station_settings SET watchlist_confirmed=0,sources_confirmed=0,auto_enabled=0,schedule_time='09:00',next_run=NULL,last_attempt_at=NULL,last_success_at=NULL,last_status='never',last_error='',last_visit_at=NULL,last_feed_visible_at=NULL,coverage_json='{}' WHERE id=1")
    for name in ("config.json", "secrets.json", "search.sqlite3"):
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


@pytest.fixture
def served_folder(tmp_path):
    handler = partial(SimpleHTTPRequestHandler, directory=tmp_path)
    with ThreadingHTTPServer(("127.0.0.1", 0), handler) as server:
        worker = threading.Thread(target=server.serve_forever)
        worker.start()
        previous = os.environ.get("PRIVATE_FEED_HOSTS")
        os.environ["PRIVATE_FEED_HOSTS"] = "127.0.0.1"
        try:
            yield f"http://127.0.0.1:{server.server_port}"
        finally:
            if previous is not None:
                os.environ["PRIVATE_FEED_HOSTS"] = previous
            else:
                os.environ.pop("PRIVATE_FEED_HOSTS")
            server.shutdown()
            worker.join()


@pytest.fixture
def raspberry_feed(tmp_path, served_folder):
    root = etree.Element("rss", version="2.0")
    channel = etree.SubElement(root, "channel")
    etree.SubElement(channel, "title").text = "News - Raspberry Pi"
    record = etree.SubElement(channel, "item")
    etree.SubElement(record, "title").text = "Citizen science with Raspberry Pi"
    etree.SubElement(record, "link").text = "https://www.raspberrypi.com/news/citizen-science-with-raspberry-pi/"
    etree.SubElement(record, "pubDate").text = "Mon, 23 Nov 2026 10:26:34 +0000"
    (tmp_path / "news.xml").write_bytes(etree.tostring(root, encoding="UTF-8", xml_declaration=True))
    return served_folder + "/news.xml"
