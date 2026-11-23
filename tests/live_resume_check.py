import argparse
import asyncio
import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


async def check(task_id, live):
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
    from pydantic import ValidationError

    from researcher import db, research, station
    from researcher.config import DATA, get_profile
    from researcher.providers import Gateway, ResearchError
    from researcher.research_engine import ResearchRun
    from researcher.research_models import Draft, DraftSubmission, PROMPT_VERSION
    from researcher.worker import run_task

    db.initialize()
    task = db.one("SELECT * FROM tasks WHERE id=?", (task_id,))
    assert task and task["kind"] == "scan" and task["status"] in {"failed", "interrupted"}
    original = db.one("SELECT * FROM research_operations WHERE task_id=? AND operation_key LIKE 'synthesis:native:%' ORDER BY id DESC LIMIT 1", (task_id,))
    assert original, "指定任务没有保存原始工具调用草稿"
    response = json.loads(original["outputs_json"])
    arguments = response["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"]
    DraftSubmission.model_validate_json(arguments)
    print("历史真实返回的 draft 对象通过完整格式校验。", flush=True)
    gateway = Gateway(get_profile(task["profile_id"]), task_id)
    try:
        with gateway.validate_result(original["operation_key"]):
            Draft.model_validate_json(arguments)
    except ValidationError:
        pass
    else:
        raise AssertionError("历史原始返回应当重现此前的格式校验错误")
    assert db.one("SELECT status FROM research_operations WHERE id=?", (original["id"],))["status"] == "invalid"

    async with AsyncSqliteSaver.from_conn_string(str(DATA / "researcher.sqlite3")) as saver:
        saved = await saver.aget_tuple({"configurable": {"thread_id": f"research:{task_id}:{task['created_at']}"}})
    state = saved.checkpoint["channel_values"]
    assert "draft" not in state and "chunks" in state
    research.retry(task_id)
    archived = db.one("SELECT * FROM research_operations WHERE task_id=? AND operation_key=?", (task_id, original["operation_key"] + ":invalid:" + str(original["id"])))
    assert archived["outputs_json"] == original["outputs_json"]
    assert db.one("SELECT status FROM research_operations WHERE id=?", (original["id"],))["status"] == "retry_authorized"
    print("历史格式错误保留原始返回，明确继续处理后才授权重新请求。", flush=True)
    db.execute("UPDATE tasks SET status='running' WHERE id=?", (task_id,))
    task = db.one("SELECT * FROM tasks WHERE id=?", (task_id,))
    run = ResearchRun(task)
    key = "synthesis:json:" + PROMPT_VERSION
    already_prepared = run.saved_request(key) is not None
    builds = 0

    async def build():
        nonlocal builds
        builds += 1
        return await run.synthesis_inputs(state)

    first = await run.prepare_request(key, build)
    await asyncio.sleep(1.1)
    restored = ResearchRun(task)
    second = await restored.prepare_request(key, build)
    assert first == second and builds == int(not already_prepared)
    print("真实证据、时间及 schema 在重新创建执行对象后保持一致。", flush=True)
    if not live:
        return

    before = db.query("SELECT operation,count(*) AS n FROM usage WHERE task_id=? GROUP BY operation", (task_id,))
    draft = await run.synthesize(state)
    used = db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (task_id,))["n"]
    assert await restored.synthesize(state) == draft
    assert db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (task_id,))["n"] == used
    print("实际模型完成草稿；额度变化后的重复处理新增 API 请求 0 次。", flush=True)
    state.update(draft)
    review = await run.review(state)
    used = db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (task_id,))["n"]
    assert await restored.review(state) == review
    assert db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (task_id,))["n"] == used
    print("实际证据核验重复处理新增 API 请求 0 次。", flush=True)
    await run_task(task)
    completed = db.one("SELECT * FROM tasks WHERE id=?", (task_id,))
    assert completed["status"] == "success", completed["error"]
    assert db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (task_id,))["n"] == used
    for item in before:
        if item["operation"] in {"plan", "search"}:
            assert db.one("SELECT count(*) AS n FROM usage WHERE task_id=? AND operation=?", (task_id, item["operation"]))["n"] == item["n"]
    assert db.one("SELECT outputs_json FROM research_operations WHERE id=?", (original["id"],))["outputs_json"] == original["outputs_json"]
    print("LangGraph 从原失败检查点恢复并保存结果：", json.loads(completed["result_json"])["outcome"], flush=True)

    prepared = run.saved_request(key)
    try:
        await run.gateway.post("/tools/search", {"model": run.profile.model, "messages": prepared["messages"]}, "synthesis", key)
    except ResearchError as error:
        assert "保存的请求与当前内容" in str(error)
    else:
        raise AssertionError("已完成操作的请求发生变化时应立即拒绝")
    assert db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (task_id,))["n"] == used

    new_id = research.enqueue("scan", json.loads(task["job_json"]))
    db.execute("UPDATE tasks SET status='running',started_at=? WHERE id=?", (db.now(), new_id))
    new_run = ResearchRun(db.one("SELECT * FROM tasks WHERE id=?", (new_id,)))
    initial = await new_run.recall({})
    plan = await new_run.plan(initial)
    used = db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (new_id,))["n"]
    assert await new_run.plan(initial) == plan
    assert db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (new_id,))["n"] == used
    print("实际规划请求恢复：新增 API 请求 0 次。", flush=True)
    planned = db.one("SELECT outputs_json FROM research_operations WHERE task_id=? AND operation_key='planner:0'", (task_id,))
    calls = json.loads(planned["outputs_json"])["choices"][0]["message"]["tool_calls"]
    searches = [call for call in calls if call["function"]["name"] == "search_web"]
    assert len(searches) >= 2
    await asyncio.gather(*(new_run.execute_tool(call, initial) for call in searches))
    for call in searches:
        operation = db.one("SELECT kind,status FROM research_operations WHERE task_id=? AND operation_key=?", (new_id, "tool:" + call["id"]))
        assert operation == {"kind": "search_web", "status": "success"}
    print("真实并发搜索分别保存工具编号和 search_web 类型。", flush=True)
    with db.connect() as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert not connection.execute("PRAGMA foreign_key_check").fetchall()


def main():
    parser = argparse.ArgumentParser(description="使用真实失败任务的隔离副本验证请求恢复；--live 实际调用已配置的服务商")
    parser.add_argument("--source", type=Path, default=ROOT / "data")
    parser.add_argument("--task-id", type=int, required=True)
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    work = ROOT / ".work"
    work.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="resume-check-", dir=work) as folder:
        target = Path(folder)
        for name in ("researcher.sqlite3", "search.sqlite3"):
            with sqlite3.connect(f"file:{args.source / name}?mode=ro", uri=True) as source:
                with sqlite3.connect(target / name) as destination:
                    source.backup(destination)
            (target / name).chmod(0o600)
        for name in ("config.json", "secrets.json"):
            shutil.copyfile(args.source / name, target / name)
            (target / name).chmod(0o600)
        os.environ["RESEARCHER_DATA_DIR"] = folder
        os.environ["RESEARCHER_MODELS_DIR"] = str(args.source / "models")
        os.environ["HF_HUB_OFFLINE"] = "1"
        os.environ["TMPDIR"] = folder
        asyncio.run(check(args.task_id, args.live))


if __name__ == "__main__":
    main()
