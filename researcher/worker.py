import asyncio
import fcntl
import logging
import os
import signal
from logging.handlers import RotatingFileHandler

from pydantic import ValidationError

from . import db, station
from .config import DATA, fingerprint, get_profile, mark_verified, prepare_data
from .providers import Cancelled, Gateway, ResearchError, YieldResearch
from .scheduler import tick

logger = logging.getLogger("researcher.worker")


async def perform(task):
    if task.get("session_id") and task["kind"] in {"interactive", "onboard", "question", "revise", "followup", "event_research"}:
        from .research_engine import perform as perform_research
        await perform_research(task)
    elif task["kind"] == "feed":
        from .ingest.jobs import perform as perform_feed
        await perform_feed(task)
    elif task["kind"] == "selectbench":
        from .ingest.benchmark import perform as perform_benchmark
        await perform_benchmark(task)
    elif task["kind"] == "briefing":
        from .ingest.briefing import perform as perform_briefing
        await perform_briefing(task)
    elif task["kind"] in {"test_chat", "test_search", "test_tools"}:
        await test_connection(task)
    else:
        raise ResearchError("该任务类型已停止执行")


async def test_connection(task):
    profile = get_profile(task["profile_id"])
    signature = task["profile_fingerprint"]
    if fingerprint(profile) != signature:
        raise ResearchError("排队期间模型配置发生变化，请重新创建任务")
    gateway = Gateway(profile, task["id"])
    gateway.checkpoint("正在连接云端服务")
    mode = task["kind"]
    sources = []
    if mode == "test_chat":
        reply = await gateway.complete([{"role": "user", "content": "请用中文简短回复连接成功。"}], max_tokens=512)
        body = reply.text
    elif mode == "test_tools":
        from .research_models import Finish
        tool = {"type": "function", "function": {"name": "finish_research", "description": "提交工具调用测试结果", "parameters": Finish.model_json_schema()}}
        reply = await gateway.complete([{"role": "user", "content": "请调用 finish_research，reason 填写工具调用验证完成，complete 为 true，needs_input 留空。"}],
                                       tools=[tool], reasoning_effort="low", max_tokens=2048, operation="test_tools")
        calls = reply.message.get("tool_calls") or []
        if len(calls) != 1 or calls[0]["function"]["name"] != "finish_research":
            raise ResearchError("模型未返回有效的原生工具调用")
        Finish.model_validate_json(calls[0]["function"]["arguments"])
        body = "原生工具调用已通过实际请求验证。"
    else:
        query = "Python 官方文档 Python 3.10 更新"
        if profile.provider in {"qwen", "volcengine"}:
            reply = await gateway.complete([{"role": "user", "content": query + "，提供原始来源链接。"}], search=True, max_tokens=512)
            sources = reply.sources
        else:
            sources = await gateway.search(query)
        if not sources:
            raise ResearchError("联网测试未取得来源")
        body = "已取得公开来源，验证了联网接口。"
    gateway.checkpoint()
    for source in sources:
        station.save_document(source, task["id"])
    mark_verified(profile, mode.removeprefix("test_"), db.now(), signature)
    with db.transaction() as connection:
        ensure_not_cancelled(connection, task["id"])
        finish(connection, task["id"], body, "测试成功，已记录验证时间")


def ensure_not_cancelled(connection, task_id):
    task = connection.execute("SELECT cancel_requested FROM tasks WHERE id=?", (task_id,)).fetchone()
    if task["cancel_requested"]:
        raise Cancelled("任务已取消")


def finish(connection, task_id, body, progress):
    connection.execute("UPDATE tasks SET status='success',result_text=?,progress=?,finished_at=?,heartbeat=? WHERE id=?", (body, progress, db.now(), db.now(), task_id))


async def run_task(task):
    operation = asyncio.create_task(perform(task))
    try:
        while not operation.done():
            await asyncio.sleep(0.5)
            row = db.one("SELECT cancel_requested FROM tasks WHERE id=?", (task["id"],))
            if row["cancel_requested"]:
                operation.cancel()
                await asyncio.gather(operation, return_exceptions=True)
                raise Cancelled("任务已取消，已发出的服务商请求仍可能产生费用")
            db.execute("UPDATE tasks SET heartbeat=? WHERE id=?", (db.now(), task["id"]))
        await operation
        row = db.one("SELECT status,cancel_requested FROM tasks WHERE id=?", (task["id"],))
        if row["status"] == "running" and row["cancel_requested"]:
            raise Cancelled("任务已取消")
    except YieldResearch:
        db.execute("UPDATE tasks SET status='queued',progress='已保存进度，等待手动研究完成后继续' WHERE id=?", (task["id"],))
    except Cancelled as error:
        db.execute("UPDATE feed_runs SET status='cancelled',error=?,finished_at=? WHERE task_id=? AND status='running'", (str(error), db.now(), task["id"]))
        db.execute("UPDATE tasks SET status='cancelled',error=?,progress='已取消',finished_at=? WHERE id=?", (str(error), db.now(), task["id"]))
    except asyncio.CancelledError:
        operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)
        if task["kind"] == "feed":
            db.execute("UPDATE tasks SET status='queued',progress='等待继续处理保存的条目' WHERE id=? AND status='running'", (task["id"],))
        else:
            db.execute("UPDATE tasks SET status='interrupted',error='后台服务停止，任务没有自动重试',finished_at=? WHERE id=? AND status='running'", (db.now(), task["id"]))
        raise
    except Exception as error:
        message = str(error) if isinstance(error, (ResearchError, ValueError)) and not isinstance(error, ValidationError) else f"研究处理失败（{type(error).__name__}），结果未保存，请检查配置或运行日志"
        if isinstance(error, ValidationError):
            fields = ", ".join(".".join(str(part) for part in item["loc"]) + ":" + item["type"] for item in error.errors()[:5])
            message = "结构化研究结果未通过校验：" + fields + "；原文和实际返回内容已保留"
        logger.error("任务 %s 执行失败，错误类型 %s", task["id"], type(error).__name__)
        db.execute("UPDATE feed_runs SET status='failed',error=?,finished_at=? WHERE task_id=? AND status='running'", (message, db.now(), task["id"]))
        db.execute("UPDATE tasks SET status='failed',error=?,progress='执行失败',finished_at=? WHERE id=?", (message, db.now(), task["id"]))
        if task["kind"] == "question":
            db.execute("UPDATE research_questions SET status='failed',updated_at=? WHERE task_id=?", (db.now(), task["id"]))
    finally:
        if task["automatic"] and (task["schedule_key"] or "").startswith("topic-revision:"):
            db.execute("UPDATE topic_retry_quotas SET status='finished' WHERE task_id=? AND status='active' "
                       "AND EXISTS(SELECT 1 FROM tasks WHERE id=? AND status NOT IN ('queued','running'))", (task["id"], task["id"]))
        if task.get("session_id"):
            current = db.one("SELECT status FROM tasks WHERE id=?", (task["id"],))
            if current and current["status"] in {"failed", "cancelled", "interrupted"}:
                db.execute("UPDATE research_sessions SET status=?,updated_at=? WHERE id=?", (current["status"], db.now(), task["session_id"]))
                from .research import finish_gap
                finish_gap(task, False)


async def heartbeat():
    while True:
        db.execute("INSERT INTO runtime(name,value) VALUES('worker_heartbeat',?) ON CONFLICT(name) DO UPDATE SET value=excluded.value", (db.now(),))
        await asyncio.sleep(3)


def claim_task(foreground):
    with db.transaction() as connection:
        task = connection.execute("SELECT * FROM tasks WHERE status='queued' AND cancel_requested=0 AND (priority>0)=? ORDER BY priority DESC,id LIMIT 1", (foreground,)).fetchone()
        if task:
            started = task["started_at"] or db.now()
            connection.execute("UPDATE tasks SET status='running',started_at=?,heartbeat=? WHERE id=?", (started, db.now(), task["id"]))
            return {**dict(task), "status": "running", "started_at": started}
    return None


async def loop():
    from .ingest.collectors import ensure_catalog
    ensure_catalog()
    db.execute("UPDATE research_cache SET status='unknown' WHERE status='running'")
    db.execute("UPDATE research_operations SET status='unknown' WHERE status='running'")
    db.execute("UPDATE tasks SET status='queued',progress='等待继续处理保存的条目' WHERE status='running' AND kind='feed' AND cancel_requested=0")
    interrupted = db.query("SELECT * FROM tasks WHERE status='running'")
    db.execute("UPDATE tasks SET status='interrupted',error='后台服务曾停止，任务没有自动重试',finished_at=? WHERE status='running'", (db.now(),))
    for task in interrupted:
        if task.get("session_id"):
            from .research import finish_gap
            db.execute("UPDATE research_sessions SET status='interrupted',updated_at=? WHERE id=?", (db.now(), task["session_id"]))
            finish_gap(task, False)
    db.execute("UPDATE feed_runs SET status='interrupted',error='采集已中断',finished_at=? WHERE status='running' AND task_id IN (SELECT id FROM tasks WHERE status='interrupted')", (db.now(),))
    db.execute("UPDATE usage SET status='interrupted' WHERE status='reserved'")
    db.execute("UPDATE topic_retry_quotas SET status='finished' WHERE status='active' AND task_id IN "
               "(SELECT id FROM tasks WHERE status NOT IN ('queued','running'))")
    pulse = asyncio.create_task(heartbeat())
    stop = asyncio.Event()
    running = {True: None, False: None}
    event_loop = asyncio.get_running_loop()
    for event in (signal.SIGTERM, signal.SIGINT):
        event_loop.add_signal_handler(event, stop.set)
    try:
        while not stop.is_set():
            tick()
            for lane, operation in running.items():
                if operation and operation.done():
                    await operation
                    running[lane] = None
            for lane in running:
                if running[lane] is None and (lane or running[True] is None):
                    task = claim_task(lane)
                    if task:
                        running[lane] = asyncio.create_task(run_task(task))
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
    except asyncio.CancelledError:
        pass
    finally:
        active = [operation for operation in running.values() if operation is not None]
        for operation in active:
            operation.cancel()
        await asyncio.gather(*active, return_exceptions=True)
        pulse.cancel()
        await asyncio.gather(pulse, return_exceptions=True)
        db.execute("DELETE FROM runtime WHERE name='worker_heartbeat'")


def main():
    os.umask(0o077)
    prepare_data()
    db.initialize()
    handler = RotatingFileHandler(DATA / "worker.log", maxBytes=1_000_000, backupCount=3)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    with (DATA / "worker.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("当前数据目录已有 worker 正在运行") from None
        logger.info("后台研究服务启动")
        asyncio.run(loop())
        logger.info("后台研究服务停止")


if __name__ == "__main__":
    main()
