import asyncio
from datetime import datetime, timedelta
import os

import httpx
import pytest

from researcher import db, station
from researcher.ingest.collectors import Entry, FeedConfig, collect, save_source, stamp
from researcher.ingest.gate import archive_cutoff, gate_date, observe, preview
from researcher.industry import selection
from researcher.providers import ResearchError, Source

RASPBERRY_ARTICLE = "https://www.raspberrypi.com/news/citizen-science-with-raspberry-pi/"
RASPBERRY_PUBLISHED = datetime.fromisoformat("2026-11-23T10:26:34+00:00")


def test_publication_boundaries():
    policy = selection()
    published = RASPBERRY_PUBLISHED
    limit = published + timedelta(hours=48)
    assert gate_date(published, limit, limit, None, False, policy) == ("discovered", "")
    assert gate_date(published, limit + timedelta(seconds=1), limit, None, False, policy) == ("gated_archive", "expired")
    assert gate_date(published, limit, limit, published, False, policy) == ("gated_archive", "published_after")
    assert gate_date(published, limit, limit, published - timedelta(seconds=1), True, policy) == ("gated_archive", "initial_backfill")
    assert gate_date(None, limit, limit, None, False, policy) == ("pending_date", "missing_date")
    assert gate_date(published, published - timedelta(seconds=1), published - timedelta(seconds=1), None, False, policy) == ("pending_date", "future_date")
    cutoff = archive_cutoff(limit, policy.backfill_months)
    assert gate_date(cutoff, limit, limit, None, True, policy) == ("gated_archive", "expired")
    assert gate_date(cutoff - timedelta(seconds=1), limit, limit, None, True, policy) == ("gated_archive", "before_backfill_window")


def test_canonical_url_and_official_publisher_prefix():
    url = "https://hailo.ai/blog/"
    official = FeedConfig(key="hailo", name="Hailo", kind="rss", tier="T1", url="https://hailo.ai/feed/", publisher="hailo", publisherUrlPrefixes=[url], enabled=True)
    other = FeedConfig(key="external-local", name="外部推送", kind="external", tier="T2", publisher="external-local")
    save_source(official)
    source_id = save_source(other)
    source = db.one("SELECT * FROM feed_sources WHERE id=?", (source_id,))
    checked = RASPBERRY_PUBLISHED + timedelta(minutes=1)
    url = "https://HAILO.ai:443/blog/next-gen-ai-accelerator-hailo-10/"
    title = "Next-generation AI accelerator Hailo-10"
    entries = [Entry(url + "?utm_source=rss#announcement", title, published_at=RASPBERRY_PUBLISHED)]
    with db.transaction() as connection:
        cursor, new, gated = observe(connection, source, entries, checked, selection())
        assert (new, gated) == (1, 1)
    with db.transaction() as connection:
        assert observe(connection, source, [Entry(url, title)], checked, selection())[1] == 0
    item, = db.query("SELECT * FROM feed_items")
    assert item["canonical_url"] == "https://hailo.ai/blog/next-gen-ai-accelerator-hailo-10/"
    assert item["publisher_key"] == "hailo" and item["tier"] == "T1"
    assert item["is_backfill"] and item["state"] == "gated_archive"
    assert cursor["baseline_at"] == stamp(checked)


@pytest.mark.parametrize("use_alias", [False, True], ids=["document-url", "alias-url"])
def test_historical_alias_reuses_archived_document_and_version(use_alias):
    published = stamp(RASPBERRY_PUBLISHED)
    source = Source("Citizen science with Raspberry Pi", RASPBERRY_ARTICLE, "Environmental monitoring with Raspberry Pi.", "公开资料摘录", published)
    document_id, old_version = station.save_document(source, None)
    _, current_version = station.save_document(Source(source.title, source.url, "Environmentalists use Raspberry Pi to monitor the world around them.", "公开资料原文", published), None)
    assert current_version != old_version
    alias = source.url + "?ref=maker-monday"
    with db.transaction() as connection:
        connection.execute("DELETE FROM source_aliases WHERE url=?", (source.url,))
        connection.execute("INSERT INTO source_aliases(url,document_id) VALUES(?,?)", (alias, document_id))
    feed = FeedConfig(key="external-local", name="外部推送", kind="external", tier="T2", publisher="external-local", enabled=True)
    source_id = save_source(feed)
    checked = RASPBERRY_PUBLISHED + timedelta(hours=1)
    entry = Entry(alias if use_alias else source.url, source.title, published_at=checked - timedelta(minutes=1))
    policy = selection()
    with db.transaction() as connection:
        row = connection.execute("SELECT * FROM feed_sources WHERE id=?", (source_id,)).fetchone()
        cursor, _, _ = observe(connection, row, [], checked, policy)
        connection.execute("UPDATE feed_sources SET cursor_json=? WHERE id=?", (station.encode(cursor), source_id))
        row = connection.execute("SELECT * FROM feed_sources WHERE id=?", (source_id,)).fetchone()
        assert observe(connection, row, [entry], checked, policy)[1:] == (1, 1)
        assert observe(connection, row, [Entry(source.url, source.title), Entry(alias, source.title)], checked, policy)[1:] == (0, 2)
    assert db.query("SELECT state,gate_reason,published_at,document_id,version_id FROM feed_items") == [
        {"state": "duplicate", "gate_reason": "historical_document", "published_at": published,
         "document_id": document_id, "version_id": current_version},
    ]
    assert not db.query("SELECT id FROM item_evaluations")


@pytest.mark.parametrize("first_has_articles", [False, True])
def test_recent_article_discovered_after_first_poll_enters_the_funnel(raspberry_feed, first_has_articles):
    source = FeedConfig(key="raspberry-pi", name="Raspberry Pi", kind="rss", tier="T1.5", url=raspberry_feed, publisher="raspberry-pi", enabled=True)
    source_id = save_source(source)
    first_poll = RASPBERRY_PUBLISHED + timedelta(minutes=10)
    policy = selection()
    historical = Entry("https://www.raspberrypi.com/news/price-rises-for-some-16gb-raspberry-pi-products/", "Price rises for some 16GB Raspberry Pi products",
                       published_at=datetime.fromisoformat("2026-11-02T00:00:00+00:00"))
    first_entries = [historical] if first_has_articles else []
    with db.transaction() as connection:
        row = connection.execute("SELECT * FROM feed_sources WHERE id=?", (source_id,)).fetchone()
        cursor, _, _ = observe(connection, row, first_entries, first_poll, policy)
        connection.execute("UPDATE feed_sources SET cursor_json=? WHERE id=?", (station.encode(cursor), source_id))

    async def read_entries():
        async with httpx.AsyncClient(trust_env=False) as client:
            _, entries = await collect(client, source)
            return entries

    entries = asyncio.run(read_entries())
    with db.transaction() as connection:
        row = connection.execute("SELECT * FROM feed_sources WHERE id=?", (source_id,)).fetchone()
        assert observe(connection, row, first_entries + entries, first_poll + timedelta(hours=1), policy)[1] == 1
    assert db.one("SELECT state,is_backfill FROM feed_items WHERE canonical_url=?", (RASPBERRY_ARTICLE,)) == {"state": "discovered", "is_backfill": 0}
    if first_has_articles:
        assert db.one("SELECT state,is_backfill FROM feed_items WHERE canonical_url=?", (historical.url,)) == {"state": "gated_archive", "is_backfill": 1}


def test_real_http_conditional_request_and_preview(raspberry_feed):
    async def read():
        source = FeedConfig(key="raspberry-pi", name="Raspberry Pi", kind="rss", tier="T1.5", url=raspberry_feed, publisher="raspberry-pi")
        async with httpx.AsyncClient(trust_env=False) as client:
            response, entries = await collect(client, source)
            assert entries[0].url == RASPBERRY_ARTICLE
            assert entries[0].published_at == RASPBERRY_PUBLISHED
            conditional = {"final_url": response.url, "last_modified": response.headers["last-modified"], "etag": ""}
            unchanged, entries = await collect(client, source, conditional)
            assert unchanged.status == 304 and not entries
            os.environ["PRIVATE_FEED_HOSTS"] = ""
            with pytest.raises(ResearchError):
                await collect(client, source)
            os.environ["PRIVATE_FEED_HOSTS"] = "127.0.0.1"
        result = await preview(source)
        assert result[0]["published_at"] == stamp(RASPBERRY_PUBLISHED)
        assert not db.query("SELECT id FROM feed_items")

    asyncio.run(read())
