import argparse
import asyncio
import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path
from contextlib import nullcontext


ROOT = Path(__file__).resolve().parents[1]


async def resume_research(task_id):
    from researcher import db, knowledge, research
    from researcher.worker import run_task
    from researcher.research_engine import ResearchRun
    db.initialize()
    original = db.one("SELECT * FROM tasks WHERE id=?", (task_id,))
    if json.loads(original["result_json"]).get("outcome") == "partial":
        saved = db.one("SELECT * FROM task_steps WHERE task_id=? AND stage='研究结果草稿'", (task_id,))
        identifiers = json.loads(saved["inputs_json"])["chunk_ids"]
        state = {"draft": json.loads(saved["outputs_json"])["draft"], "chunks": [{**knowledge.chunk_row(identifier), "chunk_id": identifier} for identifier in identifiers], "memory": await asyncio.to_thread(knowledge.context, original["prompt"]), "round": json.loads(original["result_json"]).get("rounds", 0), "finish": {}, "errors": []}
        db.execute("UPDATE tasks SET status='running' WHERE id=?", (task_id,))
        run = ResearchRun(db.one("SELECT * FROM tasks WHERE id=?", (task_id,)))
        state.update(await run.review(state))
        await run.commit(state)
    else:
        research.retry(task_id)
        db.execute("UPDATE tasks SET status='running' WHERE id=?", (task_id,))
        await run_task(db.one("SELECT * FROM tasks WHERE id=?", (task_id,)))
    task = db.one("SELECT * FROM tasks WHERE id=?", (task_id,))
    assert task["status"] == "success", task["error"]
    result = json.loads(task["result_json"])
    assert result["complete"], result.get("completion_notes")
    print("真实研究恢复完成：", task_id, flush=True)
    repeated_id = research.enqueue("interactive", json.loads(task["job_json"]))
    db.execute("UPDATE tasks SET status='running',started_at=? WHERE id=?", (db.now(), repeated_id))
    await run_task(db.one("SELECT * FROM tasks WHERE id=?", (repeated_id,)))
    repeated = db.one("SELECT * FROM tasks WHERE id=?", (repeated_id,))
    assert repeated["status"] == "success", repeated["error"]
    assert json.loads(repeated["result_json"])["reused_task_id"] == task_id
    assert not db.one("SELECT id FROM usage WHERE task_id=?", (repeated_id,))
    print("重复提问：新增API请求0次。", flush=True)


async def check_saved_history(live, http, models):
    from researcher import continuity, db, knowledge, research
    from researcher.providers import Gateway, ResearchError, UnknownOutcome
    from researcher.config import get_profile
    from researcher.worker import run_task
    from researcher.research_engine import ResearchRun

    db.initialize()
    original_claims = db.one("SELECT count(*) AS n FROM claims")["n"]
    original_documents = db.one("SELECT count(*) AS n FROM documents")["n"]
    first = continuity.consolidate_history()
    second = continuity.consolidate_history()
    assert not any(second.values()), second
    assert db.one("SELECT count(*) AS n FROM claims")["n"] >= original_claims
    assert db.one("SELECT count(*) AS n FROM documents")["n"] >= original_documents
    assert not db.query("SELECT e.claim_id FROM claim_evidence e JOIN claims c ON c.id=e.claim_id JOIN document_chunks ch ON ch.id=e.chunk_id WHERE c.document_version_id!=ch.version_id")
    for row in db.query("SELECT id FROM knowledge_items WHERE canonical_item_id IS NOT NULL"):
        assert continuity.canonical_knowledge(row["id"])["canonical_item_id"] is None
    item = db.one("SELECT k.* FROM knowledge_items k JOIN knowledge_evidence e ON e.item_id=k.id WHERE k.canonical_item_id IS NULL ORDER BY k.id LIMIT 1")
    claims = [row["claim_id"] for row in db.query("SELECT claim_id FROM knowledge_evidence WHERE item_id=?", (item["id"],))]
    facets = db.query("SELECT * FROM knowledge_facets WHERE item_id=?", (item["id"],))
    for facet in facets:
        assert knowledge.put_knowledge(item["body"], facet["kind"], claims, None, json.loads(facet["conditions_json"]), facet["topic_id"], facet["entity_id"]) == item["id"]
    print("历史整理与重复保存：", first, flush=True)

    goal = "根据公开原始资料核对 NXP Ara240 的 PCIe 与 USB 主机接口版本，保留厂商来源属性。最多两条发现、一条回答要点。"
    task_id = research.enqueue("interactive" if live else "revise", {"main_question": goal})
    db.execute("UPDATE tasks SET status='running',started_at=? WHERE id=?", (db.now(), task_id))
    task = db.one("SELECT * FROM tasks WHERE id=?", (task_id,))
    actual_chunk = db.one("SELECT c.* FROM document_chunks c JOIN documents d ON d.id=(SELECT document_id FROM document_versions WHERE id=c.version_id) WHERE c.version_id=(SELECT id FROM document_versions WHERE document_id=d.id AND content_hash=d.content_hash) ORDER BY c.id LIMIT 1")
    document = knowledge.chunk_row(actual_chunk["id"])
    calls = 0

    async def read_saved(cache_key):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        return knowledge.source_result(document["document_id"], document["version_id"], document["url"])

    inputs = {"url": document["url"]}
    results = await asyncio.gather(*(continuity.cached_operation("read_evidence", inputs, {}, task_id, read_saved) for _ in range(2)))
    assert calls == 1 and sum(result["reused"] for result in results) == 1
    replay = await continuity.cached_operation("read_evidence", inputs, {}, task_id, read_saved, force=True)
    assert replay["reused"] and calls == 1
    print("相同操作并发与同任务恢复：实际读取1次，后续复用。", flush=True)

    other_id = research.enqueue("interactive" if live else "revise", {"main_question": goal})
    db.execute("UPDATE tasks SET status='running',started_at=? WHERE id=?", (db.now(), other_id))
    reused = await continuity.cached_operation("read_evidence", inputs, {}, other_id, read_saved)
    assert reused["reused"] and calls == 1
    refreshed = await continuity.cached_operation("read_evidence", inputs, {}, other_id, read_saved, force=True)
    assert not refreshed["reused"] and calls == 2
    print("跨任务复用与主动刷新：通过。", flush=True)

    started = asyncio.Event()
    async def interrupted(cache_key):
        started.set()
        await asyncio.Event().wait()
    pending = asyncio.create_task(continuity.cached_operation("read_evidence", {"chunk_id": actual_chunk["id"]}, {}, task_id, interrupted))
    await started.wait()
    pending.cancel()
    await asyncio.gather(pending, return_exceptions=True)
    try:
        await continuity.cached_operation("read_evidence", {"chunk_id": actual_chunk["id"]}, {}, task_id, interrupted)
    except UnknownOutcome:
        pass
    else:
        raise AssertionError("中断结果未知的操作不应自动重发")
    print("真实取消后的未知状态阻止重发：通过。", flush=True)

    failed_reads = 0
    missing_id = db.one("SELECT max(id)+1 AS id FROM document_chunks")["id"]
    async def read_missing(cache_key):
        nonlocal failed_reads
        failed_reads += 1
        if knowledge.chunk_row(missing_id) is None:
            raise ResearchError("原文片段不存在")
        raise AssertionError("检查时不应产生新的片段")
    failures = await asyncio.gather(*(continuity.cached_operation("read_evidence", {"chunk_id": missing_id}, {}, task_id, read_missing) for _ in range(2)), return_exceptions=True)
    assert failed_reads == 1 and all(isinstance(error, ResearchError) for error in failures)
    print("相同失败操作并发：只执行1次，保留错误。", flush=True)

    # 使用已保存的草稿和核验结果重放提交。
    saved = db.one("SELECT s.task_id,s.inputs_json,s.outputs_json,r.outputs_json AS review_json FROM task_steps s JOIN task_steps r ON r.task_id=s.task_id AND r.stage='逐项证据核验' JOIN tasks t ON t.id=s.task_id WHERE s.stage='研究结果草稿' AND t.status='success' AND t.cancel_requested=0 ORDER BY s.task_id DESC LIMIT 1")
    assert saved, "历史数据库中没有可用于重放的真实核验结果"
    original = db.one("SELECT * FROM tasks WHERE id=?", (saved["task_id"],))
    draft = json.loads(saved["outputs_json"])["draft"]
    chunk_ids = json.loads(saved["inputs_json"])["chunk_ids"]
    state = {"draft": draft, "review": json.loads(saved["review_json"]), "chunks": [{**knowledge.chunk_row(identifier), "chunk_id": identifier} for identifier in chunk_ids], "round": json.loads(original["result_json"]).get("rounds", 0), "finish": {}, "errors": []}
    replay_id = research.enqueue("revise", json.loads(original["job_json"]))
    db.execute("UPDATE tasks SET status='running',started_at=? WHERE id=?", (db.now(), replay_id))
    replay_run = ResearchRun(db.one("SELECT * FROM tasks WHERE id=?", (replay_id,)))
    await replay_run.commit(state)
    first_count = db.one("SELECT count(*) AS n FROM knowledge_items")["n"]
    db.execute("UPDATE tasks SET status='running' WHERE id=?", (replay_id,))
    await replay_run.commit(state)
    assert db.one("SELECT count(*) AS n FROM knowledge_items")["n"] == first_count
    print("真实历史草稿和核验结果重复提交：知识数量保持不变。", flush=True)

    if models or http:
        indexed = await asyncio.to_thread(knowledge.ensure_index, False, [document["version_id"]])
        matches = await asyncio.to_thread(knowledge.related_knowledge, "NXP Ara240 PCIe USB 主机接口", 5)
        assert matches and all(item["canonical_item_id"] is None for item in matches)
        print("真实原文索引与 BGE 知识召回：", indexed, flush=True)
    if http:
        url = "https://raw.githubusercontent.com/nxp-imx-support/llm-edge-studio/main/README.md"
        fetched = await knowledge.fetch(url, task_id, force=True)
        assert fetched["chunks"] and fetched["http_status"] == 200
        checked = await knowledge.fetch(url, other_id, force=True)
        assert checked["version_id"] == fetched["version_id"]
        assert checked["http_status"] in {200, 304}
        assert checked["change_type"] == "existing"
        print("真实公开原文两次获取：版本相同，第二次 HTTP", checked["http_status"], flush=True)

    if live:
        gateway = Gateway(get_profile(), task_id)
        query = "NXP Ara240 DNPU PCIe USB host interface official"
        before = db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (task_id,))["n"]
        sources = await asyncio.gather(gateway.search(query, "dedup-live:one"), gateway.search(query, "dedup-live:two"))
        after = db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (task_id,))["n"]
        assert sources[0] and sources[0] == sources[1] and after - before == 1
        other_gateway = Gateway(get_profile(), other_id)
        assert await other_gateway.search(query, "dedup-live:cross") == sources[0]
        assert not db.one("SELECT id FROM usage WHERE task_id=?", (other_id,))
        print("真实搜索：同轮并发2次、跨任务1次，共1次API请求。", flush=True)
        await run_task(task)
        completed = db.one("SELECT * FROM tasks WHERE id=?", (task_id,))
        assert completed["status"] == "success", completed["error"]
        result = json.loads(completed["result_json"])
        assert result["complete"] and result["changes"], result.get("completion_notes")
        repeated_id = research.enqueue("interactive", {"main_question": goal})
        db.execute("UPDATE tasks SET status='running',started_at=? WHERE id=?", (db.now(), repeated_id))
        await run_task(db.one("SELECT * FROM tasks WHERE id=?", (repeated_id,)))
        repeated = db.one("SELECT * FROM tasks WHERE id=?", (repeated_id,))
        assert repeated["status"] == "success", repeated["error"]
        repeated_result = json.loads(repeated["result_json"])
        assert repeated_result["reused_task_id"] == task_id
        assert not db.one("SELECT id FROM usage WHERE task_id=?", (repeated_id,))
        print("真实研究及重复提问：完成，重复提问新增API请求0次。", flush=True)
    with db.connect() as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert not connection.execute("PRAGMA foreign_key_check").fetchall()


def main():
    parser = argparse.ArgumentParser(description="在真实历史数据库的隔离副本验证去重；--live 实际调用当前服务商")
    parser.add_argument("--source", type=Path, default=ROOT / "data")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--http", action="store_true")
    parser.add_argument("--models", action="store_true")
    parser.add_argument("--device", choices=["cpu", "cuda:0"])
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--resume-task", type=int)
    args = parser.parse_args()
    work = ROOT / ".work"
    work.mkdir(exist_ok=True)
    if args.resume_task and not args.work_dir:
        parser.error("--resume-task 需要指定 --work-dir")
    if args.work_dir:
        args.work_dir = args.work_dir.resolve()
        if not args.work_dir.is_relative_to(work.resolve()):
            parser.error("检查目录必须位于项目 .work 中")
        if not args.resume_task:
            args.work_dir.mkdir(mode=0o700)
    context = nullcontext(str(args.work_dir)) if args.work_dir else tempfile.TemporaryDirectory(prefix="dedup-check-", dir=work)
    with context as folder:
        target = Path(folder)
        if not args.resume_task:
            source = sqlite3.connect(f"file:{args.source / 'researcher.sqlite3'}?mode=ro", uri=True)
            destination = sqlite3.connect(target / "researcher.sqlite3")
            source.backup(destination)
            source.close()
            destination.close()
            if (args.source / "search.sqlite3").exists():
                source_index = sqlite3.connect(f"file:{args.source / 'search.sqlite3'}?mode=ro", uri=True)
                target_index = sqlite3.connect(target / "search.sqlite3")
                source_index.backup(target_index)
                source_index.close()
                target_index.close()
            for name in ("config.json", "secrets.json"):
                shutil.copyfile(args.source / name, target / name)
                (target / name).chmod(0o600)
        if args.device:
            config = json.loads((target / "config.json").read_text())
            config.setdefault("research", {})["device"] = args.device
            (target / "config.json").write_text(json.dumps(config, ensure_ascii=False))
        os.environ["RESEARCHER_DATA_DIR"] = str(target)
        os.environ["RESEARCHER_MODELS_DIR"] = str(args.source / "models")
        os.environ["HF_HUB_OFFLINE"] = "1"
        os.environ["TMPDIR"] = folder
        asyncio.run(resume_research(args.resume_task) if args.resume_task else check_saved_history(args.live, args.http, args.models))


if __name__ == "__main__":
    main()
