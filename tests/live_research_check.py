import argparse
import asyncio
import json
import time
from pathlib import Path

from fastapi.testclient import TestClient

from researcher import db, knowledge, research, station
from researcher.app import app
from researcher.config import DATA
from researcher.ingest.jobs import enqueue as enqueue_feed
from researcher.research_engine import ResearchRun
from researcher.worker import run_task


GOAL = "根据已保存的 NXP Ara240 中文规格资料，说明它支持的模型类别、目标应用与部署条件。保留厂商来源属性，明确资料没有给出的条件。已有资料足够时结束调查。"


async def execute(task_id):
    started = time.monotonic()
    with db.transaction() as connection:
        owned = connection.execute("UPDATE tasks SET status='running',started_at=coalesce(started_at,?),heartbeat=? WHERE id=? AND status='queued'", (db.now(), db.now(), task_id)).rowcount == 1
    if owned:
        await run_task(db.one("SELECT * FROM tasks WHERE id=?", (task_id,)))
    else:
        while db.one("SELECT status FROM tasks WHERE id=?", (task_id,))["status"] == "running":
            if time.monotonic() - started > 7500:
                raise RuntimeError("等待实际 worker 完成任务超时")
            await asyncio.sleep(.5)
    current = db.one("SELECT * FROM tasks WHERE id=?", (task_id,))
    if current["status"] != "success":
        raise RuntimeError(current["error"])
    result = station.decode(current["result_json"])
    print(json.dumps({"task_id": task_id, "outcome": result.get("outcome"), "requests": db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (task_id,))["n"], "knowledge": len(result.get("knowledge_ids", [])), "rounds": result.get("rounds"), "seconds": round(time.monotonic() - started, 2)}, ensure_ascii=False), flush=True)
    return current, result


def evaluate_retrieval():
    cases = json.loads((Path(__file__).parent / "research_cases.json").read_text())
    successes = 0
    for case in cases:
        results = knowledge.search(case["question"], limit=10)
        matched = any(case["source"].casefold() in item["url"].casefold() for item in results)
        successes += int(matched)
        print(json.dumps({"question": case["question"], "found": matched, "documents": list(dict.fromkeys(item["document_id"] for item in results))}, ensure_ascii=False), flush=True)
    score = successes / len(cases)
    print(f"真实资料检索 Recall@10：{successes}/{len(cases)} = {score:.1%}", flush=True)
    assert score >= .85


async def main(stage):
    db.initialize()
    with TestClient(app, base_url="http://localhost") as client:
        assert client.get("/research").status_code == 200
        csrf = client.cookies["researcher_csrf"]
        if stage == "reuse":
            completed = db.one("SELECT * FROM tasks WHERE status='success' AND json_extract(result_json,'$.complete')=1 ORDER BY id DESC LIMIT 1")
            if not completed:
                raise RuntimeError("没有已完成的真实研究可用于复用检查")
            repeated = research.enqueue("interactive", station.decode(completed["job_json"]))
            task = db.one("SELECT * FROM tasks WHERE id=?", (repeated,))
            if not ResearchRun(task).reusable_result():
                client.post(f"/tasks/{repeated}/cancel", data={"csrf": csrf})
                raise RuntimeError("最近研究的来源、作用范围或完成标准不满足复用条件")
            _, reused = await execute(repeated)
            assert reused["reused_task_id"] == completed["id"]
            assert not db.query("SELECT id FROM usage WHERE task_id=?", (repeated,))
        if stage in {"retrieval", "all"}:
            await asyncio.to_thread(evaluate_retrieval)
        if stage in {"research", "all", "resume"}:
            response = client.post("/research", data={"csrf": csrf, "message": GOAL, **({"force_refresh": "yes"} if stage == "resume" else {})}, follow_redirects=False)
            assert response.status_code == 303, response.text
            location = response.headers["location"]
            if "error=" in location:
                raise RuntimeError(location)
            session_id = int(location.rsplit("/", 1)[1])
            task = db.one("SELECT * FROM tasks WHERE session_id=? ORDER BY id DESC LIMIT 1", (session_id,))
            if stage == "resume":
                db.execute("UPDATE tasks SET status='running',started_at=? WHERE id=?", (db.now(), task["id"]))
                task = db.one("SELECT * FROM tasks WHERE id=?", (task["id"],))
                running = asyncio.create_task(run_task(task))
                while not running.done():
                    if db.one("SELECT id FROM research_operations WHERE task_id=? AND kind='plan' AND status='success'", (task["id"],)):
                        running.cancel()
                        await asyncio.gather(running, return_exceptions=True)
                        break
                    await asyncio.sleep(.1)
                before = db.one("SELECT count(*) AS n FROM usage WHERE task_id=? AND operation='plan' AND status='success'", (task["id"],))["n"]
                saved_operation = db.one("SELECT operation_key,outputs_json FROM research_operations WHERE task_id=? AND kind='plan' AND status='success' ORDER BY id LIMIT 1", (task["id"],))
                assert saved_operation and before > 0
                research.retry(task["id"])
                await execute(task["id"])
                after = db.one("SELECT count(*) AS n FROM research_operations WHERE task_id=? AND kind='plan' AND status='success'", (task["id"],))["n"]
                assert after >= before
                assert db.one("SELECT operation_key,outputs_json FROM research_operations WHERE task_id=? AND operation_key=?", (task["id"], saved_operation["operation_key"])) == saved_operation
            else:
                completed, result = await execute(task["id"])
                assert result["points"], completed["result_text"]
                assert result["knowledge_ids"]
                assert result["document_ids"]
                assert client.get(location).status_code == 200
                for identifier in result["document_ids"]:
                    assert client.get(f"/documents/{identifier}").status_code == 200
                if result["complete"]:
                    repeated = research.enqueue("interactive", {"main_question": GOAL, "purpose": "行业竞争", "completion": "形成有原文依据的结论、适用条件和仍待核实的问题"})
                    _, reused = await execute(repeated)
                    assert reused["reused_task_id"] == task["id"]
                    assert db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (repeated,))["n"] == 0
                else:
                    raise RuntimeError("真实规格资料问题尚未完整回答：" + completed["result_text"])
        if stage in {"collect", "all"}:
            task_id = enqueue_feed()
            _, result = await execute(task_id)
            assert db.query("SELECT id FROM feed_runs WHERE task_id=?", (task_id,))
            assert not db.query("SELECT id FROM usage WHERE task_id=? AND operation='search'", (task_id,))
            assert client.get("/").status_code == 200
        if stage in {"revise", "all"}:
            topic = db.one("SELECT * FROM topics WHERE research_state='active' ORDER BY id LIMIT 1")
            if not topic:
                raise RuntimeError("当前资料没有已确认专题")
            task_id = research.enqueue("revise", {"topic_id": topic["id"]})
            await execute(task_id)
            assert not db.query("SELECT id FROM usage WHERE task_id=? AND operation='search'", (task_id,))
        if stage == "cancel":
            task_id = research.enqueue("interactive", {"main_question": GOAL})
            assert client.post(f"/tasks/{task_id}/cancel", data={"csrf": csrf}, follow_redirects=False).status_code == 303
            assert db.one("SELECT status FROM tasks WHERE id=?", (task_id,))["status"] == "cancelled"
            assert not db.query("SELECT id FROM usage WHERE task_id=?", (task_id,))
    with db.connect() as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert not connection.execute("PRAGMA foreign_key_check").fetchall()
    print(f"阶段 {stage} 完成，实际资料目录：{DATA}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="使用真实资料和当前配置验证 AI 研究能力，research/collect/revise/resume 会调用 API")
    parser.add_argument("--stage", choices=["retrieval", "research", "collect", "revise", "resume", "reuse", "cancel", "all"], default="retrieval")
    asyncio.run(main(parser.parse_args().stage))
