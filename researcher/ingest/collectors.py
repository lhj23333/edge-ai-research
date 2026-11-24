import asyncio
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from email.utils import parsedate_to_datetime
from typing import Literal
from urllib.parse import urljoin, urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from cssselect import SelectorError
from lxml import etree, html
from lxml.cssselect import CSSSelector
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .. import continuity, db, knowledge, station
from ..industry import Tier, source_catalog
from ..providers import ResearchError

FEED_KINDS = ("rss", "web_list", "json_list", "external")
ACCEPT = {
    "rss": "application/rss+xml,application/atom+xml,application/xml,text/xml",
    "web_list": "text/html,application/xhtml+xml",
    "json_list": "application/json",
}
DATE_PROBE_BYTES = 128 * 1024
DATE_PROBE_SECONDS = 5
SOURCE_HAS_HISTORY = """(
 EXISTS(SELECT 1 FROM feed_runs r WHERE r.source_id=s.id) OR
 EXISTS(SELECT 1 FROM feed_items i WHERE i.source_id=s.id) OR
 EXISTS(SELECT 1 FROM feed_observations o WHERE o.source_id=s.id)
)"""


class FeedOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item: str = ""
    title: str = ""
    url: str = ""
    href_attribute: str = "href"
    date: str = ""
    date_attribute: str = "datetime"
    date_format: str = ""
    date_timezone: str = "UTC"
    summary: str = ""
    detail_date: str = ""
    items_path: str = ""
    exclude: list[str] = Field(default_factory=list)
    allowed_prefixes: list[str] = Field(default_factory=list)


class FeedConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,79}$")
    name: str = Field(min_length=1, max_length=150)
    kind: Literal["rss", "web_list", "json_list", "external"]
    tier: Tier
    url: str = Field(default="", max_length=2500)
    publisher: str = Field(min_length=1, max_length=150)
    owner: str = Field(default="", max_length=120)
    category: str = Field(default="", max_length=80)
    published_after: str = ""
    publisherUrlPrefixes: list[str] = Field(default_factory=list)
    options: FeedOptions = Field(default_factory=FeedOptions)
    enabled: bool = False

    @model_validator(mode="after")
    def validate_configuration(self):
        if self.kind != "external":
            self.url = continuity.canonical_url(self.url)
        elif self.url:
            raise ValueError("外部推送源无需填写请求地址")
        for prefix in self.publisherUrlPrefixes + self.options.allowed_prefixes:
            continuity.canonical_url(prefix)
        try:
            ZoneInfo(self.options.date_timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("信源时区必须使用有效的 IANA 时区名称") from exc
        if self.published_after:
            start = parse_date(self.published_after, self.options.date_timezone)
            if start is None:
                raise ValueError("监控起点必须为 ISO 日期或日期时间")
            self.published_after = stamp(start)
        if self.kind == "web_list" and not self.options.item:
            raise ValueError("网页列表需要条目 CSS 选择器 item")
        selectors = [self.options.detail_date]
        if self.kind == "web_list":
            selectors += [self.options.item, self.options.title, self.options.url, self.options.date, self.options.summary]
        try:
            for selector in filter(None, selectors):
                CSSSelector(selector)
        except SelectorError as exc:
            raise ValueError("网页选择器格式无效") from exc
        return self


class PushEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str = Field(min_length=1, max_length=2500)
    title: str = Field(min_length=1, max_length=400)
    summary: str = Field(default="", max_length=5000)
    published_at: str | None = Field(default=None, max_length=120)


@dataclass(frozen=True)
class Entry:
    url: str
    title: str
    summary: str = ""
    published_at: datetime | None = None


def stamp(value):
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_date(value, zone="UTC", format_string=""):
    if not isinstance(value, str) or not value.strip():
        return None
    value = value.strip()
    try:
        if format_string:
            parsed = datetime.strptime(value, format_string)
        elif len(value) == 10:
            parsed = datetime.combine(date.fromisoformat(value), time.min)
        else:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (ValueError, TypeError, OverflowError):
            return None
    return parsed.replace(tzinfo=parsed.tzinfo or ZoneInfo(zone)).astimezone(timezone.utc)


def allowed_hosts():
    return {value.strip().lower().rstrip(".") for value in os.environ.get("PRIVATE_FEED_HOSTS", "").split(",") if value.strip()}


def detail_hosts(source):
    host = urlsplit(source.url).hostname
    return allowed_hosts() & {host} if host else set()


def matches_prefix(url, prefix):
    source, target = map(urlsplit, (continuity.canonical_url(prefix), continuity.canonical_url(url)))
    path = source.path.rstrip("/")
    return (source.scheme, source.netloc) == (target.scheme, target.netloc) and (target.path == path or target.path.startswith(path + "/"))


def source_config(row):
    return FeedConfig.model_validate({**station.decode(row["config_json"]), "key": row["source_key"], "publisher": row["publisher_key"],
                                     **{key: row[key] for key in ("name", "kind", "tier", "url", "category", "enabled")},
                                     "published_after": row["published_after"] or ""})


def save_source(source, source_id=None, *, initial=False):
    timestamp = db.now()
    owner = station.entity("厂商", source.owner) if source.owner else None
    details = source.model_dump(include={"owner", "options", "publisherUrlPrefixes"}, exclude_defaults=True)
    values = (source.key, source.name, source.kind, source.tier, source.category, source.url, source.publisher, owner,
              station.encode(details), source.published_after or None, int(source.enabled))
    with db.transaction() as connection:
        if source_id is None:
            if initial:
                exists = connection.execute("SELECT id FROM feed_sources WHERE source_key=?", (source.key,)).fetchone()
                if exists:
                    return exists[0]
                if connection.execute("SELECT 1 FROM feed_source_lifecycle WHERE action='delete' AND source_key=?", (source.key,)).fetchone():
                    return None
            # 排队任务可能仍引用已删除的信源编号。
            return connection.execute("""
                INSERT INTO feed_sources(id,source_key,name,kind,tier,category,url,publisher_key,owner_entity_id,config_json,published_after,enabled,created_at,updated_at)
                VALUES(max(coalesce((SELECT max(id) FROM feed_sources),0),
                           coalesce((SELECT max(source_id) FROM feed_source_lifecycle),0))+1,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (*values, timestamp, timestamp)).lastrowid
        previous = connection.execute("SELECT * FROM feed_sources WHERE id=?", (source_id,)).fetchone()
        if not previous:
            raise ValueError("信源不存在")
        if previous["archived_at"]:
            raise ValueError("请先恢复已归档信源，再编辑配置")
        if previous["source_key"] != source.key:
            raise ValueError("已保存信源的唯一标识不能修改")
        changed = any(previous[field] != value for field, value in (("kind", source.kind), ("url", source.url)))
        # 仅更换地址或类型时重建首次导入基线。
        cursor = "{}" if changed else previous["cursor_json"]
        connection.execute("UPDATE feed_sources SET source_key=?,name=?,kind=?,tier=?,category=?,url=?,publisher_key=?,owner_entity_id=?,config_json=?,published_after=?,enabled=?,cursor_json=?,next_poll_at=NULL,updated_at=?,revision=revision+1 WHERE id=?", (*values, cursor, timestamp, source_id))
        connection.execute("DELETE FROM source_checks WHERE feed_source_id=?", (source_id,))
        return source_id


def change_source_lifecycle(source_id, action):
    with db.transaction() as connection:
        source = connection.execute(f"SELECT s.*, {SOURCE_HAS_HISTORY} AS has_history FROM feed_sources s WHERE id=?", (source_id,)).fetchone()
        if not source:
            raise ValueError("信源不存在")
        timestamp = db.now()
        if action == "delete":
            if source["has_history"]:
                raise ValueError("此信源已有采集历史，无法删除。请改用归档。")
            connection.execute("DELETE FROM source_checks WHERE feed_source_id=?", (source_id,))
            connection.execute("DELETE FROM feed_sources WHERE id=?", (source_id,))
        else:
            if action == "archive" and not source["has_history"]:
                raise ValueError("此信源没有采集历史，可以直接删除。")
            if bool(source["archived_at"]) == (action == "archive"):
                return
            archived_at = timestamp if action == "archive" else None
            connection.execute("UPDATE feed_sources SET archived_at=?,enabled=0,next_poll_at=NULL,updated_at=?,revision=revision+1 WHERE id=?",
                               (archived_at, timestamp, source_id))
        connection.execute("INSERT INTO feed_source_lifecycle(source_id,source_key,action,changed_at) VALUES(?,?,?,?)",
                           (source_id, source["source_key"], action, timestamp))


def ensure_catalog():
    existing = {row["source_key"] for row in db.query(
        "SELECT source_key FROM feed_sources UNION SELECT source_key FROM feed_source_lifecycle WHERE action='delete'")}
    for entry in source_catalog():
        if entry["key"] not in existing:
            save_source(FeedConfig.model_validate(entry), initial=True)


def plain(value):
    if not isinstance(value, str):
        return ""
    if "<" in value:
        value = html.fragment_fromstring(value, create_parent=True).text_content()
    return " ".join(value.split())


def xml_entries(body):
    root = etree.fromstring(body, etree.XMLParser(resolve_entities=False, no_network=True, recover=False))
    names = {"rss", "feed", "RDF"}
    if etree.QName(root).localname not in names:
        raise ResearchError("信源没有返回 RSS 或 Atom 文档")
    xpath = "./channel/item | ./*[local-name()='entry' or local-name()='item']"
    for row in root.xpath(xpath):
        link = row.xpath("string(./*[local-name()='link' and (not(@rel) or @rel='alternate')][1]/@href)")
        url = link or row.xpath("string(./*[local-name()='link'][1])")
        title = row.xpath("string(./*[local-name()='title'][1])")
        summary = row.xpath("string(./*[local-name()='description' or local-name()='summary'][1])")
        published = row.xpath("string(./*[local-name()='pubDate' or local-name()='published' or local-name()='date'][1])")
        yield url, title, summary, published


def selected_node(node, selector):
    return next(iter(node.cssselect(selector)), None) if selector else node


def node_value(node, attr=""):
    return ((node.get(attr) if attr else None) or node.text_content()) if node is not None else ""


def web_entries(body, options):
    root = html.fromstring(body)
    for row in root.cssselect(options.item):
        link = selected_node(row, options.url) if options.url else row if row.tag == "a" else selected_node(row, "a[href]")
        if link is None:
            continue
        title = selected_node(row, options.title) if options.title else link
        summary = selected_node(row, options.summary) if options.summary else None
        date_node = selected_node(row, options.date) if options.date else None
        yield link.get(options.href_attribute, ""), node_value(title), node_value(summary), node_value(date_node, options.date_attribute)


def field(value, path):
    for part in path.split(".") if path else ():
        value = value.get(part) if isinstance(value, dict) else None
    return value


def json_entries(body, options):
    entries = field(json.loads(body), options.items_path)
    if not isinstance(entries, list) or any(not isinstance(row, dict) for row in entries):
        raise ResearchError("JSON 信源没有返回所配置的对象数组")
    for row in entries:
        if not any(field(row, key) for key in options.exclude):
            yield tuple(field(row, path) for path in (options.url or "html_url", options.title or "name", options.summary or "body", options.date or "published_at"))


def entries_from(raw, source, base):
    options = source.options
    seen = set()
    for url, title, summary, published in raw:
        if not isinstance(url, str) or not url.strip() or not plain(title):
            continue
        try:
            original = urljoin(base, url.strip())
            canonical = continuity.canonical_url(original)
        except ValueError:
            continue
        if len(canonical) > 2500 or canonical in seen or (options.allowed_prefixes and not any(matches_prefix(canonical, prefix) for prefix in options.allowed_prefixes)):
            continue
        seen.add(canonical)
        yield Entry(original, plain(title)[:400], plain(summary)[:5000], parse_date(published, options.date_timezone, options.date_format))


def parse_collection(resource, source):
    if resource.status == 304:
        return []
    if source.kind == "rss":
        raw = xml_entries(resource.body)
    elif source.kind == "web_list":
        raw = web_entries(resource.body, source.options)
    elif source.kind == "json_list":
        raw = json_entries(resource.body, source.options)
    else:
        raise ResearchError("外部信源通过提交接口接收条目")
    return list(entries_from(raw, source, resource.url))


def push_entries(value, source):
    if not isinstance(value, list) or len(value) > 200:
        raise ValueError("推送内容需要是最多 200 条的 JSON 数组")
    entries = [PushEntry.model_validate(item) for item in value]
    parsed = list(entries_from(((item.url, item.title, item.summary, item.published_at) for item in entries), source, ""))
    if entries and not parsed:
        raise ValueError("提交内容没有通过信源的链接与标题检查")
    return parsed


async def collect(client, source, checked=None):
    response = await knowledge.get_resource(client, source.url, checked=checked, allowed_hosts=allowed_hosts(), accept=ACCEPT[source.kind])
    return response, await asyncio.to_thread(parse_collection, response, source)


async def article(client, source, url, checked=None):
    return await knowledge.get_resource(client, url, checked=checked, allowed_hosts=detail_hosts(source),
                                        accept="text/html,application/xhtml+xml,application/pdf,text/plain,text/markdown")


async def article_date(client, source, url):
    try:
        resource = await asyncio.wait_for(
            knowledge.get_resource(client, url, allowed_hosts=detail_hosts(source), accept=ACCEPT["web_list"],
                                   max_bytes=DATE_PROBE_BYTES, prefix_only=True),
            timeout=DATE_PROBE_SECONDS,
        )
    except asyncio.TimeoutError as exc:
        raise ResearchError("读取发布日期元数据超时") from exc
    return await asyncio.to_thread(detail_date, resource, source)


def structured_dates(value):
    if isinstance(value, dict):
        if value.get("datePublished"):
            yield value["datePublished"]
        if "@graph" in value:
            yield from structured_dates(value["@graph"])
    elif isinstance(value, list):
        for element in value:
            yield from structured_dates(element)


def detail_date(resource, source):
    if not resource.body or "html" not in resource.headers.get("content-type", "text/html"):
        return None
    root = html.fromstring(resource.body)
    options = source.options
    values = root.xpath("//meta[@property='article:published_time' or @itemprop='datePublished']/@content | //time[@itemprop='datePublished']/@datetime")
    if options.detail_date:
        values.insert(0, node_value(selected_node(root, options.detail_date), options.date_attribute))
    for script in root.xpath("//script[@type='application/ld+json']/text()"):
        try:
            values.extend(structured_dates(json.loads(script)))
        except json.JSONDecodeError:
            continue
    for value in values:
        published = parse_date(value, options.date_timezone) or parse_date(value, options.date_timezone, options.date_format)
        if published:
            return published
    return None
