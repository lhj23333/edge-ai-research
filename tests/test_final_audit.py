import asyncio
import json
import os
import secrets
import ssl
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
from tokenizers import Tokenizer

from researcher import continuity, db, knowledge, research, station
from researcher.config import Profile, save_profile, settings, write_json
from researcher.providers import Gateway, ResearchError, Source, YieldResearch
from researcher.research_engine import ResearchRun
from researcher.research_models import Draft, Finding, FindingReview, JudgmentProposal, Point, PointReview, Review, UpdateReview, tool_definitions
from researcher.worker import claim_task


HAILO_URL = "https://hailo.ai/products/ai-accelerators/hailo-10h-ai-accelerator/"
HAILO_TITLE = "Hailo-10H AI Accelerator"
HAILO_EXCERPT = "Hailo-10H supports edge generative AI applications."


@pytest.fixture
def local_gateway(tmp_path, monkeypatch):
    certificate, private_key = tmp_path / "localhost.pem", tmp_path / "localhost.key"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-subj", "/CN=127.0.0.1", "-addext", "subjectAltName=IP:127.0.0.1",
                    "-keyout", str(private_key), "-out", str(certificate)], check=True, capture_output=True)
    monkeypatch.setenv("SSL_CERT_FILE", str(certificate))
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, private_key)
    received = []
    endpoints = {}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            received.append(self.path)
            status, result = endpoints[self.path](request)
            body = json.dumps(result).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        server.socket = context.wrap_socket(server.socket, server_side=True)
        worker = threading.Thread(target=server.serve_forever)
        worker.start()
        try:
            yield f"https://127.0.0.1:{server.server_port}", endpoints, received
        finally:
            server.shutdown()
            worker.join()


def configure_profile(base_url):
    profile = Profile(id="deepseek", name="DeepSeek", provider="deepseek", base_url=base_url, model="deepseek-chat",
                      chat_verified=db.now(), search_verified=db.now(), input_price=2, output_price=4, search_price=0.01)
    save_profile(profile, secrets.token_urlsafe())
    return profile


@pytest.mark.parametrize(("kind", "automatic", "allowed"), [
    ("followup", True, False), ("event_research", True, False), ("revise", True, False),
    ("interactive", False, True), ("question", False, True), ("event_research", False, True),
])
def test_search_registration_and_execution_follow_manual_permission(local_gateway, kind, automatic, allowed):
    base_url, endpoints, received = local_gateway
    configure_profile(base_url)
    config = settings()
    config["research"]["device"] = "cpu"
    write_json("config.json", config)

    def search_archive(request):
        records = db.query("SELECT title,url,content FROM documents WHERE instr(lower(title),lower(?))>0 LIMIT ?", (request["query"], request["count"]))
        return 200, {"results": records}

    endpoints["/tools/search"] = search_archive
    task_id = research.enqueue(kind, {"main_question": "查询 Hailo-10H 的公开说明", "target_id": 1}, automatic=automatic)
    task = claim_task(not automatic)
    assert task["id"] == task_id
    run = ResearchRun(task)
    definitions = {item["function"]["name"] for item in run.planner_tools(tool_definitions())}
    assert {"search_knowledge", "read_evidence"} <= definitions
    assert ("search_web" in definitions) is allowed
    call = {"id": "search-hailo", "function": {"name": "search_web", "arguments": '{"query":"Hailo-10H"}'}}
    if not allowed:
        assert "read_source" not in definitions
        with pytest.raises(ResearchError, match="未启用联网搜索"):
            asyncio.run(run.execute_tool(call, {"visited": []}))
        assert received == [] and not db.query("SELECT id FROM usage")
        return
    result = asyncio.run(run.execute_tool(call, {"visited": []}))
    assert result["status"] == "no_results"
    assert received == ["/tools/search"]
    assert db.one("SELECT operation,status FROM usage WHERE task_id=?", (task_id,)) == {"operation": "search", "status": "success"}


def test_research_at_page_cap_reads_all_counted_search_results(local_gateway, served_folder, tmp_path):
    base_url, endpoints, received = local_gateway
    configure_profile(base_url)
    config = settings()
    config["cost_profile"] = "eco"
    config["research"]["device"] = "cpu"
    write_json("config.json", config)
    excerpts = (
        "Hailo-10H is an edge AI accelerator. Product information includes support for generative AI models.",
        "Hailo-10H is offered in an M.2 form factor. The host must provide a compatible slot.",
        "Hailo-10H executes AI inference at the edge. The host application integrates with Hailo software.",
        "Hailo-10H supports large language model inference. Deployment should follow the model and platform documentation.",
        "Hailo-10H has an additional product document. The search did not select this resource.",
    )
    files = [tmp_path / f"hailo-{index}.txt" for index in range(len(excerpts))]
    for path, body in zip(files, excerpts, strict=True):
        path.write_text(body)
    urls = [f"{served_folder}/{path.name}" for path in files]

    def search_archive(request):
        assert request["count"] == 4
        return 200, {"results": [{"title": path.stem, "url": url, "summary": path.read_text().split(". ")[0]}
                                 for path, url in zip(files[:4], urls[:4], strict=True)]}

    endpoints["/tools/search"] = search_archive
    task_id = research.enqueue("interactive", {"main_question": "核对 Hailo-10H 模块的部署条件"})
    run = ResearchRun(claim_task(True))

    def tool(name, identifier, **arguments):
        return {"id": identifier, "function": {"name": name, "arguments": station.encode(arguments)}}

    async def exercise():
        state = {"visited": [], "messages": [], "round": 1, "no_progress": 0, "errors": []}
        searched = await run.execute_tool(tool("search_web", "find", query="Hailo-10H"), state)
        assert [item["url"] for item in searched["documents"]] == urls[:4]
        assert {item["extent"] for item in searched["documents"]} == {"搜索摘要"}
        assert run.remaining_pages() == 0
        assert not run.tool_allowed("search_web") and run.tool_allowed("read_source")

        config["limits"]["daily_cost"] = 0.000001
        write_json("config.json", config)
        assert (await run.plan(state))["degraded_reason"] == "cost_safety_limit"
        planned = run.saved_request(f"planner:{run.prompt_version}:1")
        plan_tools = {item["function"]["name"]: item["function"]["description"] for item in planned["tools"]}
        assert "search_web" not in plan_tools and "reserved_web_urls" in plan_tools["read_source"]
        progress = station.decode(planned["messages"][-1]["content"])
        assert progress["remaining_web_pages"] == 0 and progress["reserved_web_urls"] == urls[:4]

        async with httpx.AsyncClient(trust_env=False) as client:
            for index, url in enumerate(urls[:4]):
                # 用本地 HTTP 的真实归档填充读取缓存，保留运行时对私有地址的限制。
                async def archive(_):
                    resource = await knowledge.get_resource(client, url, allowed_hosts=("127.0.0.1",))
                    document, version, _ = knowledge.archive_resource(resource, url, task_id)
                    await asyncio.to_thread(knowledge.ensure_index, False, [version])
                    return knowledge.source_result(document, version, url)

                await continuity.cached_operation("read_source", {"url": url}, {}, task_id, archive)
                fetched = await run.execute_tool(tool("read_source", f"read-{index}", url=url), state)
                assert "原文" in fetched["extent"] and excerpts[index] in fetched["chunks"][0]["content"]

        rejected = await run.execute_tool(tool("read_source", "new", url=urls[4]), state)
        assert "只能读取本次已计数的 URL" in rejected["error"]
        assert not continuity.document_for_url(urls[4]) and run.remaining_pages() == 0

    asyncio.run(exercise())
    assert received == ["/tools/search"]
    assert len(db.query("SELECT id FROM research_operations WHERE task_id=? AND kind='web_page'", (task_id,))) == 4


def test_private_note_turn_saves_only_task_and_session_result(client, csrf, topic):
    configure_profile("https://api.deepseek.com/v1")
    topic_form = {"csrf": csrf, "kind": "专题", "name": topic["title"], "question": topic["question"]}
    assert client.post("/watchlist/add", data=topic_form, follow_redirects=False).status_code == 303
    note_body = "本次仅整理我个人记录的设备选型意见和机房部署条件。"
    note_form = {"csrf": csrf, "target_kind": "topic", "target_id": topic["id"], "body": note_body}
    assert client.post("/annotations", data=note_form, follow_redirects=False).status_code == 303
    note_id = db.one("SELECT id FROM annotations")["id"]
    form = {"csrf": csrf, "message": "请结合本次选择的批注核对 Hailo-10H 原文", "note_id": str(note_id)}
    assert client.post("/research", data=form, follow_redirects=False).status_code == 303
    first = claim_task(True)
    run = ResearchRun(first)
    assert run.base_context()["selected_personal_notes"][0]["body"] == note_body
    assert not {"search_web", "read_source"} & {item["function"]["name"] for item in run.planner_tools(tool_definitions())}

    document, version = station.save_document(Source(HAILO_TITLE, HAILO_URL, HAILO_EXCERPT), None)
    station.save_claim(document, version, HAILO_EXCERPT, HAILO_EXCERPT, "公开披露", manual_excerpt=True)
    chunk_id = db.one("SELECT id FROM document_chunks WHERE version_id=?", (version,))["id"]
    chunk = knowledge.chunk_row(chunk_id)
    assert HAILO_EXCERPT in chunk["content"]
    findings = [Finding(ref="hailo", statement=HAILO_EXCERPT, label="公开披露", chunk_ids=[chunk_id])]
    points = [Point(category="分析判断", text=HAILO_EXCERPT, finding_refs=["hailo"])]
    judgments = [JudgmentProposal(topic_id=topic["id"], purpose="部署决策", conclusion=HAILO_EXCERPT, status="初步判断",
                                   reason="产品资料明确说明适用的应用类型", supporting_refs=["hailo"])]
    reason = "厂商产品资料直接说明了这一用途"
    draft = Draft(findings=findings, points=points, judgments=judgments, complete=True, completion_notes=reason)
    review = Review(findings=[FindingReview(ref="hailo", verdict="supported", reason=reason)],
                    points=[PointReview(index=0, verdict="supported", reason=reason)],
                    updates=[UpdateReview(index=0, category="judgments", verdict="supported", reason=reason)],
                    completion_satisfied=True, completion_notes=note_body)
    public_tables = ("documents", "document_versions", "source_snapshots", "document_chunks", "document_task_links", "ingestion_queue",
                     "claims", "claim_evidence", "evidence_processing", "knowledge_items", "knowledge_changes", "judgments", "judgment_versions", "judgment_evidence")
    before = {table: db.query(f"SELECT * FROM {table}") for table in public_tables}
    state = {"chunks": [{**chunk, "chunk_id": chunk_id}], "draft": draft.model_dump(), "review": review.model_dump(), "errors": [], "finish": {}, "round": 1}
    result = asyncio.run(run.commit(state))["outcome"]
    assert note_body in result["answer"] and f"#chunk-{chunk_id}" in result["answer"]
    assert result["complete"] and not result["knowledge_ids"] and not result["revision_ids"]
    assert {table: db.query(f"SELECT * FROM {table}") for table in public_tables} == before
    saved = db.one("SELECT result_text FROM tasks WHERE id=?", (first["id"],))
    assert saved["result_text"] == result["answer"]
    assert db.one("SELECT body FROM research_messages WHERE task_id=? AND role='assistant'", (first["id"],))["body"] == result["answer"]

    assert client.post(f"/research/{first['session_id']}/messages", data={"csrf": csrf, "message": "继续核查厂商公开说明"}, follow_redirects=False).status_code == 303
    second = claim_task(True)
    assert second["session_id"] == first["session_id"] and second["id"] != first["id"]
    public_run = ResearchRun(second)
    context = public_run.base_context()
    assert context["selected_personal_notes"] == [] and context["conversation"] == []
    assert note_body not in station.encode(context)
    state["review"] = review.model_copy(update={"completion_notes": reason}).model_dump()
    public = asyncio.run(public_run.commit(state))["outcome"]
    assert public["knowledge_ids"] and public["revision_ids"]
    assert db.one("SELECT task_id FROM claim_evidence WHERE task_id=?", (second["id"],))


@pytest.mark.parametrize("http_status", [429, 503])
def test_background_gateway_yields_after_backoff_and_resumes_without_losing_usage(local_gateway, http_status):
    base_url, endpoints, received = local_gateway
    profile = configure_profile(base_url)
    model_dir = Path(os.environ.get("RESEARCHER_TEST_MODELS_DIR", knowledge.MODELS)) / "models--BAAI--bge-m3"
    revision = (model_dir / "refs/main").read_text().strip()
    tokenizer = Tokenizer.from_file(str(model_dir / "snapshots" / revision / "tokenizer.json"))
    document_id, _ = station.save_document(Source(HAILO_TITLE, HAILO_URL, HAILO_EXCERPT), None)

    def read_archive(request):
        title = db.one("SELECT title FROM documents WHERE id=?", (request["document_id"],))["title"]
        inputs = len(tokenizer.encode(request["query"], add_special_tokens=False).ids)
        outputs = len(tokenizer.encode(title, add_special_tokens=False).ids)
        return {"title": title, "usage": {"input_tokens": inputs, "output_tokens": outputs}}

    def limited_archive(request):
        if received.count("/backoff") == 1:
            return http_status, {"usage": {"input_tokens": len(tokenizer.encode(request["query"], add_special_tokens=False).ids), "output_tokens": 0}}
        return 200, read_archive(request)

    endpoints.update({"/archive": lambda request: (200, read_archive(request)), "/backoff": limited_archive})
    task_id = research.enqueue("followup", {"main_question": "核对 Hailo 公开产品说明"}, automatic=True)
    assert claim_task(False)["id"] == task_id
    gateway = Gateway(profile, task_id)
    query = {"document_id": document_id, "query": "Hailo-10H"}

    async def wait_for_retry():
        while not db.one("SELECT id FROM usage WHERE task_id=? AND status='retry'", (task_id,)):
            await asyncio.sleep(0.01)

    async def exercise_gateway():
        first = await gateway.post("/archive", query, "plan", "already-paid")
        assert first["title"] == HAILO_TITLE
        in_flight = asyncio.create_task(gateway.post("/backoff", query, "review", "during-backoff"))
        await asyncio.wait_for(wait_for_retry(), timeout=5)
        manual = research.enqueue("interactive", {"main_question": "检查新提交的 Hailo 研究"})
        with pytest.raises(YieldResearch):
            await in_flight
        assert received == ["/archive", "/backoff"]
        assert db.one("SELECT status FROM research_operations WHERE task_id=? AND operation_key='during-backoff'", (task_id,))["status"] == "retry_pending"
        previous_usage = db.query("SELECT status,input_tokens,output_tokens,estimate FROM usage WHERE task_id=? ORDER BY id", (task_id,))
        inputs, outputs = first["usage"]["input_tokens"], first["usage"]["output_tokens"]
        assert previous_usage == [
            {"status": "success", "input_tokens": inputs, "output_tokens": outputs, "estimate": (2 * inputs + 4 * outputs) / 1_000_000},
            {"status": "retry", "input_tokens": inputs, "output_tokens": 0, "estimate": 2 * inputs / 1_000_000},
        ]
        assert claim_task(True)["id"] == manual
        db.execute("UPDATE tasks SET status='cancelled',cancel_requested=1 WHERE id=?", (manual,))
        assert (await gateway.post("/backoff", query, "review", "during-backoff"))["title"] == HAILO_TITLE
        assert received == ["/archive", "/backoff", "/backoff"]
        assert db.query("SELECT status,input_tokens,output_tokens,estimate FROM usage WHERE task_id=? ORDER BY id", (task_id,))[:2] == previous_usage

    asyncio.run(exercise_gateway())
