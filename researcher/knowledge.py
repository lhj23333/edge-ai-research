import asyncio
import hashlib
import ipaddress
import io
import json
import os
import socket
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import httpx
import jieba
import numpy as np
import sqlite_vec
import trafilatura
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pypdf import PdfReader

from . import continuity, db, station
from .config import DATA, ResearchSettings, settings
from .providers import ResearchError, Source, source_url

INDEX_LOCK = threading.RLock()
MODEL_LOCK = threading.RLock()
INDEX_VERSION = "bge-chunks-1"
MODELS = Path(os.environ.get("RESEARCHER_MODELS_DIR", DATA / "models")).resolve()


def options():
    return ResearchSettings(**settings()["research"])


@lru_cache(maxsize=2)
def embedding_model(name, device):
    from sentence_transformers import SentenceTransformer
    root = MODELS
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    model = SentenceTransformer(name, device=device, cache_folder=str(root), trust_remote_code=False)
    model.max_seq_length = 8192
    model.tokenizer.model_max_length = 8192
    return model


@lru_cache(maxsize=2)
def reranking_model(name, device):
    from sentence_transformers import CrossEncoder
    return CrossEncoder(name, device=device, cache_folder=str(MODELS), max_length=512, trust_remote_code=False)


@lru_cache(maxsize=1)
def tokenizer():
    instance = jieba.Tokenizer()
    folder = MODELS
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    instance.tmp_dir = str(folder)
    instance.initialize()
    return instance


def terms(value):
    return " ".join(part.casefold() for part in tokenizer().cut_for_search(value) if part.strip())


def index_connection(dimension):
    connection = sqlite3.connect(DATA / "search.sqlite3", timeout=30)
    connection.row_factory = sqlite3.Row
    connection.enable_load_extension(True)
    sqlite_vec.load(connection)
    connection.enable_load_extension(False)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
    connection.execute("CREATE TABLE IF NOT EXISTS chunk_hashes(id INTEGER PRIMARY KEY,hash TEXT NOT NULL)")
    connection.execute("CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(title,terms)")
    connection.execute(f"CREATE VIRTUAL TABLE IF NOT EXISTS chunks_vec USING vec0(embedding float[{dimension}])")
    return connection


def chunk_row(identifier):
    return db.one("SELECT c.*,d.id AS document_id,d.title,d.url,d.published_at,v.extent,v.fetched_at,v.verification FROM document_chunks c JOIN document_versions v ON v.id=c.version_id JOIN documents d ON d.id=v.document_id WHERE c.id=?", (identifier,))


def ensure_index(rebuild=False, version_ids=None):
    config = options()
    with INDEX_LOCK, MODEL_LOCK:
        model = embedding_model(config.embedding_model, config.device)
        dimension = model.get_embedding_dimension()
        signature = station.encode([INDEX_VERSION, config.embedding_model, dimension])
        with index_connection(dimension) as index:
            old = index.execute("SELECT value FROM metadata WHERE key='signature'").fetchone()
            if old and old[0] != signature and not rebuild:
                raise ResearchError("本地检索模型已变更，请运行 researcher index --rebuild 重建索引")
            if rebuild:
                index.execute("DROP TABLE chunks_vec")
                index.execute(f"CREATE VIRTUAL TABLE chunks_vec USING vec0(embedding float[{dimension}])")
                index.execute("DELETE FROM chunks_fts")
                index.execute("DELETE FROM chunk_hashes")
            index.execute("INSERT OR REPLACE INTO metadata VALUES('signature',?)", (signature,))
            splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(model.tokenizer, chunk_size=512, chunk_overlap=64, separators=["\n\n", "\n", "。", ". ", " ", ""])
            version_filter = " AND v.id IN (" + ",".join("?" for _ in version_ids) + ")" if version_ids else ""
            pending = db.query("SELECT v.*,d.title FROM document_versions v JOIN documents d ON d.id=v.document_id LEFT JOIN ingestion_queue q ON q.version_id=v.id WHERE (q.status IS NULL OR q.status='pending')" + version_filter + " ORDER BY v.id", tuple(version_ids or []))
            for version in pending:
                documents = splitter.split_text(version["content"]) if version["content"].strip() else []
                snapshot = db.one("SELECT headers_json FROM source_snapshots WHERE version_id=? ORDER BY id DESC LIMIT 1", (version["id"],))
                pages = station.decode(snapshot["headers_json"]).get("page_spans", []) if snapshot else []
                previous_start = -1
                with db.transaction() as connection:
                    for position, document in enumerate(documents):
                        start = version["content"].find(document, previous_start + 1)
                        if start < 0:
                            raise ResearchError("资料片段与原文位置不吻合")
                        previous_start = start
                        page = next((item["page"] for item in pages if item["start"] <= start < item["end"]), None)
                        connection.execute("INSERT OR IGNORE INTO document_chunks(version_id,position,start_offset,end_offset,page,heading,content,content_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?)", (version["id"], position, start, start + len(document), page, version["title"], document, hashlib.sha256(document.encode()).hexdigest(), db.now()))
                    connection.execute("INSERT INTO ingestion_queue(version_id,status,detail,updated_at) VALUES(?,'indexed',?,?) ON CONFLICT(version_id) DO UPDATE SET status='indexed',detail=excluded.detail,updated_at=excluded.updated_at", (version["id"], f"已建立 {len(documents)} 个原文片段", db.now()))
            chunks = db.query("SELECT c.*,d.title FROM document_chunks c JOIN document_versions v ON v.id=c.version_id JOIN documents d ON d.id=v.document_id ORDER BY c.id")
            hashes = {item["id"]: hashlib.sha256((item["content"] + item["title"]).encode()).hexdigest() for item in chunks}
            indexed = {row[0]: row[1] for row in index.execute("SELECT id,hash FROM chunk_hashes")}
            for identifier, digest in list(indexed.items()):
                if hashes.get(identifier) != digest:
                    index.execute("DELETE FROM chunks_vec WHERE rowid=?", (identifier,))
                    index.execute("DELETE FROM chunks_fts WHERE rowid=?", (identifier,))
                    index.execute("DELETE FROM chunk_hashes WHERE id=?", (identifier,))
                    del indexed[identifier]
            missing = [chunk for chunk in chunks if chunk["id"] not in indexed]
            for start in range(0, len(missing), 16):
                batch = missing[start:start + 16]
                vectors = model.encode([item["content"] for item in batch], batch_size=8, normalize_embeddings=True, convert_to_numpy=True, show_progress_bar=False)
                for item, vector in zip(batch, vectors):
                    index.execute("INSERT INTO chunks_fts(rowid,title,terms) VALUES(?,?,?)", (item["id"], terms(item["title"]), terms(item["content"])))
                    index.execute("INSERT INTO chunks_vec(rowid,embedding) VALUES(?,?)", (item["id"], np.asarray(vector, dtype=np.float32).tobytes()))
                    index.execute("INSERT INTO chunk_hashes VALUES(?,?)", (item["id"], hashes[item["id"]]))
            index.commit()
        return {"documents": len(pending), "new_chunks": len(missing), "chunks": len(chunks), "model": config.embedding_model, "device": config.device}


def search(query, limit=None, include_history=False):
    config = options()
    limit = min(limit or config.context_chunks, 40)
    ensure_index()
    with INDEX_LOCK, MODEL_LOCK:
        model = embedding_model(config.embedding_model, config.device)
        vector = model.encode([query], normalize_embeddings=True, convert_to_numpy=True, show_progress_bar=False)[0]
        with index_connection(model.get_embedding_dimension()) as index:
            query_terms = list(dict.fromkeys(part for part in terms(query).split() if len(part) > 1))[:30]
            lexical = [row[0] for row in index.execute("SELECT rowid FROM chunks_fts WHERE chunks_fts MATCH ? ORDER BY bm25(chunks_fts) LIMIT 40", (" OR ".join('"' + part.replace('"', '""') + '"' for part in query_terms),))] if query_terms else []
            semantic = [row[0] for row in index.execute("SELECT rowid,distance FROM chunks_vec WHERE embedding MATCH ? AND k=40 ORDER BY distance", (np.asarray(vector, dtype=np.float32).tobytes(),))]
        scores = {}
        for ranking in (lexical, semantic):
            for rank, identifier in enumerate(ranking):
                scores[identifier] = scores.get(identifier, 0) + 1 / (60 + rank)
        entity_ids = [row["id"] for row in db.query("SELECT id FROM entities WHERE state!='archived' AND instr(lower(?),lower(name))>0", (query,))]
        if entity_ids:
            related = db.query("SELECT DISTINCT ce.chunk_id FROM event_entities ee JOIN event_claims ec ON ec.event_id=ee.event_id JOIN claim_evidence ce ON ce.claim_id=ec.claim_id WHERE ee.entity_id IN (" + ",".join("?" for _ in entity_ids) + ")", tuple(entity_ids))
            for row in related:
                scores[row["chunk_id"]] = scores.get(row["chunk_id"], 0) + 1 / 60
        candidates = []
        for identifier in sorted(scores, key=scores.get, reverse=True)[:60]:
            chunk = chunk_row(identifier)
            if not chunk:
                continue
            latest = current_version(chunk["document_id"])
            if not include_history and chunk["version_id"] != latest:
                continue
            candidates.append(chunk)
        if not candidates:
            return []
        reranker = reranking_model(config.reranker_model, config.device)
        ranked = reranker.predict([(query, item["content"]) for item in candidates], batch_size=8, show_progress_bar=False)
        for item, score in zip(candidates, ranked):
            item["relevance"] = float(score)
            item["chunk_id"] = item.pop("id")
        candidates.sort(key=lambda item: item["relevance"], reverse=True)
        counts, selected, seen_content = {}, [], {}
        for item in candidates:
            content_key = continuity.content_identity(item["content"])
            if content_key in seen_content:
                seen_content[content_key].setdefault("same_content_sources", []).append({"document_id": item["document_id"], "url": item["url"], "version_id": item["version_id"]})
                continue
            count = counts.get(item["document_id"], 0)
            if count < max(3, limit // 3):
                selected.append(item)
                seen_content[content_key] = item
                counts[item["document_id"]] = count + 1
            if len(selected) >= limit:
                break
        return selected


def current_version(document_id):
    version = db.one("SELECT v.id FROM documents d JOIN document_versions v ON v.document_id=d.id AND v.content_hash=d.content_hash WHERE d.id=?", (document_id,))
    return version["id"] if version else None


def invalidate(document_id, reason):
    with db.transaction() as connection:
        ids = [row[0] for row in connection.execute("SELECT DISTINCT ke.item_id FROM knowledge_evidence ke JOIN claims c ON c.id=ke.claim_id WHERE c.document_id=?", (document_id,))]
        for identifier in ids:
            item = connection.execute("SELECT * FROM knowledge_items WHERE id=?", (identifier,)).fetchone()
            if item["status"] in {"needs_review", "merged"}:
                continue
            version = item["revision"] + 1
            connection.execute("UPDATE knowledge_items SET status='needs_review',revision=?,updated_at=? WHERE id=?", (version, db.now(), identifier))
            connection.execute("INSERT INTO knowledge_versions(item_id,version,body,status,reason,created_at) VALUES(?,?,?,'needs_review',?,?)", (identifier, version, item["body"], reason, db.now()))
        connection.execute("UPDATE judgments SET status='待核实' WHERE current_version_id IN (SELECT je.version_id FROM judgment_evidence je JOIN claims c ON c.id=je.claim_id WHERE c.document_id=?)", (document_id,))


async def public_url(value):
    url = source_url(value)
    parts = urlsplit(url)
    if parts.port and parts.port not in (80, 443):
        raise ResearchError("仅能读取公开网页的标准 HTTP 或 HTTPS 端口")
    loop = asyncio.get_running_loop()
    records = await loop.getaddrinfo(parts.hostname, parts.port or (443 if parts.scheme == "https" else 80), type=socket.SOCK_STREAM)
    if not records or any(not ipaddress.ip_address(record[4][0]).is_global for record in records):
        raise ResearchError("资料地址必须指向公开互联网，不能访问本机或内部网络")
    return url


def extract_resource(body, content_type, url):
    if "pdf" in content_type or urlsplit(url).path.lower().endswith(".pdf"):
        reader = PdfReader(io.BytesIO(body))
        pages, spans, offset = [], [], 0
        for number, page in enumerate(reader.pages, 1):
            stream = page.get_contents()
            if stream and len(stream.get_data()) > 30_000_000:
                raise ResearchError("PDF 页面内容超过处理上限")
            text = page.extract_text(extraction_mode="layout") or ""
            pages.append(text)
            spans.append({"page": number, "start": offset, "end": offset + len(text) + 2})
            offset += len(text) + 2
        return "\n\n".join(pages), urlsplit(url).path.rsplit("/", 1)[-1], spans, "PDF 原文（程序提取，未人工核验）"
    if "html" in content_type:
        document = trafilatura.bare_extraction(body, url=url, include_tables=True, include_links=True, include_comments=False)
        if document is None:
            raise ResearchError("网页没有可提取的正文")
        return document.text or "", document.title or url, [], "网页原文（程序提取，未人工核验）"
    if content_type.startswith("text/") or "markdown" in content_type:
        return body.decode("utf-8"), url, [], "文本原文（程序提取，未人工核验）"
    raise ResearchError("资料格式不支持；当前支持网页、文本及文本型 PDF")


def source_result(document_id, version_id, url, **metadata):
    version = db.one("SELECT extent FROM document_versions WHERE id=?", (version_id,))
    chunks = db.query("SELECT id FROM document_chunks WHERE version_id=? ORDER BY position", (version_id,))
    return {"document_id": document_id, "version_id": version_id, "url": url, "extent": version["extent"], "chunks": [{**chunk_row(item["id"]), "chunk_id": item["id"]} for item in chunks], **metadata}


async def fetch(url, task_id, force=False):
    canonical = continuity.canonical_url(url)
    async def produce(cache_key):
        return await fetch_uncached(canonical, task_id)
    result = await continuity.cached_operation("read_source", {"url": canonical}, {}, task_id, produce, force=force)
    db.execute("INSERT OR IGNORE INTO document_task_links(document_id,task_id) VALUES(?,?)", (result["document_id"], task_id))
    return result


async def fetch_uncached(url, task_id):
    config = options()
    current = await public_url(url)
    existing = continuity.document_for_url(current)
    checked = db.one("SELECT * FROM source_checks WHERE document_id=?", (existing["id"],)) if existing else None
    headers = {"User-Agent": "Researcher/0.2 (public-source research)", "Accept": "text/html,application/pdf,text/plain,text/markdown"}
    async with httpx.AsyncClient(timeout=httpx.Timeout(45, connect=15), follow_redirects=False, trust_env=False) as client:
        for _ in range(6):
            conditional = dict(headers)
            if checked and current == checked["final_url"]:
                if checked["etag"]:
                    conditional["If-None-Match"] = checked["etag"]
                if checked["last_modified"]:
                    conditional["If-Modified-Since"] = checked["last_modified"]
            async with client.stream("GET", current, headers=conditional) as response:
                if response.status_code == 304:
                    if not checked or conditional == headers:
                        raise ResearchError("来源返回未修改状态，但缺少可核对的原文版本")
                    db.execute("UPDATE source_checks SET checked_at=? WHERE document_id=?", (db.now(), checked["document_id"]))
                    return source_result(checked["document_id"], checked["version_id"], url, change_type="existing", http_status=304)
                if response.is_redirect:
                    current = await public_url(urljoin(current, response.headers["location"]))
                    continue
                response.raise_for_status()
                pieces, size = [], 0
                async for piece in response.aiter_bytes():
                    size += len(piece)
                    if size > config.max_source_bytes:
                        raise ResearchError("资料超过单份原文大小限制")
                    pieces.append(piece)
                raw = b"".join(pieces)
                content_type = response.headers.get("content-type", "text/html")
                text, title, spans, extent = await asyncio.to_thread(extract_resource, raw, content_type, current)
                if not text.strip():
                    raise ResearchError("原文没有可提取文字，不能据此形成证据")
                document_id, version_id = station.save_document(Source(title, current, text, extent), task_id)
                for alias in {url, current}:
                    db.execute("INSERT INTO source_aliases(url,document_id) VALUES(?,?) ON CONFLICT(url) DO UPDATE SET document_id=excluded.document_id", (continuity.canonical_url(alias), document_id))
                metadata = {key: response.headers[key] for key in ("content-type", "etag", "last-modified") if key in response.headers}
                metadata.update(final_url=current, page_spans=spans)
                db.execute("INSERT OR IGNORE INTO source_snapshots(version_id,content_type,content_hash,body,headers_json,method,created_at) VALUES(?,?,?,?,?,'direct_http',?)", (version_id, content_type, hashlib.sha256(raw).hexdigest(), raw, station.encode(metadata), db.now()))
                db.execute("INSERT INTO source_checks(document_id,version_id,checked_at,final_url,etag,last_modified) VALUES(?,?,?,?,?,?) ON CONFLICT(document_id) DO UPDATE SET version_id=excluded.version_id,checked_at=excluded.checked_at,final_url=excluded.final_url,etag=excluded.etag,last_modified=excluded.last_modified", (document_id, version_id, db.now(), current, metadata.get("etag", ""), metadata.get("last-modified", "")))
                await asyncio.to_thread(ensure_index, False, [version_id])
                change_type = "new" if not existing else "existing" if existing["content_hash"] == hashlib.sha256(text.encode()).hexdigest() else "updated"
                return source_result(document_id, version_id, url, change_type=change_type, http_status=200)
    raise ResearchError("资料重定向次数超过限制")


def put_knowledge(body, kind, claim_ids, task_id, conditions=None, topic_id=None, entity_id=None, reason="新增经核验的资料", existing_id=None, change_type="new", revalidated=False):
    if not claim_ids:
        raise ResearchError("保存领域知识必须提供证据")
    conditions = conditions or {}
    previous = continuity.canonical_knowledge(existing_id) if existing_id else None
    if existing_id and not previous:
        raise ResearchError("核验结果引用的已有知识不存在")
    if not previous:
        previous = next((item for item in db.query("SELECT * FROM knowledge_items WHERE canonical_item_id IS NULL") if continuity.content_identity(item["body"]) == continuity.content_identity(body) and continuity.item_compatible(item["id"], conditions, entity_id)), None)
    merge = previous and change_type not in {"updated", "conflict"}
    if merge and not continuity.item_compatible(previous["id"], conditions, entity_id, reviewed=existing_id is not None):
        raise ResearchError("相同知识的适用条件或研究对象不同，不能合并")
    identity = continuity.digest([continuity.content_identity(body), continuity.fact_conditions(conditions), entity_id])
    with db.transaction() as connection:
        if merge:
            identifier = previous["id"]
            old_claims = {row[0] for row in connection.execute("SELECT claim_id FROM knowledge_evidence WHERE item_id=?", (identifier,))}
            actual_change = "evidence" if set(claim_ids) - old_claims else "existing"
            blocked = connection.execute("SELECT 1 FROM knowledge_evidence e JOIN claims c ON c.id=e.claim_id WHERE e.item_id=? AND c.verification='人工核对发现不符' LIMIT 1", (identifier,)).fetchone()
            if previous["status"] == "needs_review" and revalidated and not blocked:
                revision = previous["revision"] + 1
                connection.execute("UPDATE knowledge_items SET status='supported',revision=?,updated_at=? WHERE id=?", (revision, db.now(), identifier))
                connection.execute("INSERT INTO knowledge_versions(item_id,version,body,status,task_id,reason,created_at) VALUES(?,?,?,'supported',?,?,?)", (identifier, revision, previous["body"], task_id, reason, db.now()))
                actual_change = "evidence"
        else:
            same = connection.execute("SELECT id FROM knowledge_items WHERE identity_key=?", (identity,)).fetchone()
            if same:
                identifier, actual_change = same[0], "existing"
            else:
                cursor = connection.execute("INSERT INTO knowledge_items(identity_key,kind,body,conditions_json,status,topic_id,entity_id,created_at,updated_at) VALUES(?,?,?,?,'supported',?,?,?,?)", (identity, kind, body, station.encode(conditions), topic_id, entity_id, db.now(), db.now()))
                identifier, actual_change = cursor.lastrowid, change_type if previous else "new"
                connection.execute("INSERT INTO knowledge_versions(item_id,version,body,status,task_id,reason,created_at) VALUES(?,1,?,'supported',?,?,?)", (identifier, body, task_id, reason, db.now()))
            if previous and previous["id"] != identifier:
                continuity.record_change(connection, task_id, identifier, actual_change, reason, previous["id"])
                if previous["status"] != "needs_review":
                    revision = previous["revision"] + 1
                    connection.execute("UPDATE knowledge_items SET status='needs_review',revision=?,updated_at=? WHERE id=?", (revision, db.now(), previous["id"]))
                    connection.execute("INSERT INTO knowledge_versions(item_id,version,body,status,task_id,reason,created_at) VALUES(?,?,?,'needs_review',?,?,?)", (previous["id"], revision, previous["body"], task_id, reason, db.now()))
        facet_key = continuity.digest([kind, conditions, topic_id, entity_id])
        connection.execute("INSERT OR IGNORE INTO knowledge_facets(item_id,facet_key,kind,conditions_json,topic_id,entity_id) VALUES(?,?,?,?,?,?)", (identifier, facet_key, kind, station.encode(conditions), topic_id, entity_id))
        for claim_id in set(claim_ids):
            connection.execute("INSERT OR IGNORE INTO knowledge_evidence(item_id,claim_id) VALUES(?,?)", (identifier, claim_id))
        continuity.record_change(connection, task_id, identifier, actual_change, reason)
    return identifier


@lru_cache(maxsize=4)
def knowledge_vectors(texts, name, device):
    return embedding_model(name, device).encode(list(texts), normalize_embeddings=True, convert_to_numpy=True, show_progress_bar=False)


def related_knowledge(query, limit=20):
    items = db.query("SELECT * FROM knowledge_items WHERE canonical_item_id IS NULL ORDER BY id")
    if not items:
        return []
    config = options()
    with MODEL_LOCK:
        vectors = knowledge_vectors(tuple(item["body"] for item in items), config.embedding_model, config.device)
        vector = embedding_model(config.embedding_model, config.device).encode([query], normalize_embeddings=True, convert_to_numpy=True, show_progress_bar=False)[0]
    scores = vectors @ vector
    order = np.argsort(-scores)[:limit]
    selected = [{**items[index], "similarity": float(scores[index])} for index in order]
    for item in selected:
        item["facets"] = db.query("SELECT kind,conditions_json,topic_id,entity_id FROM knowledge_facets WHERE item_id=?", (item["id"],))
        item["review_history"] = db.query("SELECT status,reason FROM knowledge_versions WHERE item_id=? ORDER BY version DESC LIMIT 2", (item["id"],))
        item["feedback"] = db.query("SELECT kind,body FROM research_feedback WHERE item_id=? ORDER BY id DESC LIMIT 5", (item["id"],))
    return selected


def context(query, scope=None):
    selected = related_knowledge(query, 30)
    judgments = db.query("SELECT j.id,j.topic_id,j.purpose,j.status,v.conclusion,v.boundaries,v.unresolved FROM judgments j JOIN judgment_versions v ON v.id=j.current_version_id")
    for item in selected[:30]:
        item["evidence"] = db.query("SELECT c.id AS claim_id,c.label,c.verification,d.url,v.fetched_at,v.id AS version_id FROM knowledge_evidence ke JOIN claims c ON c.id=ke.claim_id JOIN document_versions v ON v.id=c.document_version_id JOIN documents d ON d.id=c.document_id WHERE ke.item_id=?", (item["id"],))
    experiences = db.query("SELECT query,last_result,useful_count,failure_count,document_ids_json,last_used_at FROM research_experiences ORDER BY last_used_at DESC")
    words = {word for word in tokenizer().cut(query) if len(word.strip()) > 1}
    experiences.sort(key=lambda item: sum(word.casefold() in item["query"].casefold() for word in words), reverse=True)
    return {"knowledge": selected[:30], "judgments": judgments, "gaps": db.query("SELECT id,question,reason,status,target_kind,target_id FROM research_gaps ORDER BY priority DESC,updated_at DESC LIMIT 30"), "experiences": experiences[:12]}


def fresh(chunk, days=None):
    check = db.one("SELECT checked_at FROM source_checks WHERE document_id=? AND version_id=?", (chunk["document_id"], chunk["version_id"]))
    return datetime.fromisoformat(check["checked_at"] if check else chunk["fetched_at"]) >= datetime.now(timezone.utc) - timedelta(days=days or options().source_fresh_days)


def prepare_models():
    config = options()
    with MODEL_LOCK:
        encoder = embedding_model(config.embedding_model, config.device)
        encoder.encode(["端侧推理加速器的软件与部署条件"], show_progress_bar=False)
        reranking_model(config.reranker_model, config.device).predict([("软件部署条件", "产品 SDK 支持模型编译与运行")], show_progress_bar=False)
    return {"embedding": config.embedding_model, "reranker": config.reranker_model, "device": config.device}
