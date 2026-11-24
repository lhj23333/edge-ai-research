import argparse
import time
from datetime import datetime, timedelta
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx

from researcher import db, station
from researcher.config import get_profile, settings
from researcher.service import usage_summary

URL = "http://127.0.0.1:8765"


def wait_task(task_id, timeout=1200):
    deadline = time.monotonic() + timeout
    previous = None
    while time.monotonic() < deadline:
        task = db.one("SELECT id,status,progress,error FROM tasks WHERE id=?", (task_id,))
        state = (task["status"], task["progress"])
        if state != previous:
            print(f"任务 {task_id}：{task['status']} · {task['progress']}", flush=True)
            previous = state
        if task["status"] not in ("queued", "running"):
            if task["status"] != "success":
                raise RuntimeError(task["error"] or "任务未成功")
            return task
        time.sleep(2)
    raise RuntimeError("等待超时，任务可在网页中查看或取消")


def submit(client, path, values):
    response = client.post(URL + path, data={"csrf": client.cookies["researcher_csrf"], **values}, follow_redirects=False)
    if response.status_code != 303:
        response.raise_for_status()
        raise RuntimeError("操作没有返回预期跳转")
    location = response.headers.get("location", "")
    destination = urlsplit(location).path
    if destination.startswith("/tasks/"):
        return int(destination.rsplit("/", 1)[1])
    if destination.startswith("/questions/"):
        item = db.one("SELECT task_id FROM research_questions WHERE id=?", (int(destination.rsplit("/", 1)[1]),))
        if item and item["task_id"]:
            return item["task_id"]
    raise RuntimeError(f"操作没有创建任务：{location}")


def main():
    parser = argparse.ArgumentParser(description="验证当前网站的真实 AI 流程，会产生调用和研究记录")
    parser.add_argument("--stage", choices=["connection", "search", "collect", "question", "revise", "schedule", "cancel", "quota"], default="collect")
    parser.add_argument("--topic", type=int)
    arguments = parser.parse_args()
    with httpx.Client(trust_env=False, timeout=20) as client:
        client.get(URL + "/settings").raise_for_status()
        if arguments.stage in ("connection", "search"):
            task_id = submit(client, "/settings/test", {"profile_id": get_profile().id, "mode": "test_chat" if arguments.stage == "connection" else "test_search"})
            wait_task(task_id)
        elif arguments.stage == "collect":
            task_id = submit(client, "/updates/collect", {})
            wait_task(task_id)
            runs = db.query("SELECT status,new_count FROM feed_runs WHERE task_id=?", (task_id,))
            assert runs and db.one("SELECT day FROM daily_briefings LIMIT 1")
            assert not db.query("SELECT id FROM usage WHERE task_id=? AND operation='search'", (task_id,))
            print({"feed_runs": runs}, flush=True)
        elif arguments.stage == "revise":
            topic = db.one("SELECT * FROM topics WHERE id=?", (arguments.topic,)) if arguments.topic else db.one("SELECT * FROM topics WHERE research_state='active' ORDER BY id LIMIT 1")
            if not topic:
                raise RuntimeError("请先确认关注专题")
            task_id = submit(client, f"/knowledge/{topic['id']}/revise", {})
            wait_task(task_id)
            result = station.decode(db.one("SELECT result_json FROM tasks WHERE id=?", (task_id,))["result_json"])
            print({"topic": topic["id"], "revisions": len(result["revision_ids"]), "questions": len(result["gap_ids"])}, flush=True)
        elif arguments.stage == "question":
            topic = db.one("SELECT * FROM topics WHERE id=?", (arguments.topic,)) if arguments.topic else db.one("SELECT * FROM topics WHERE research_state='active' ORDER BY id LIMIT 1")
            if not topic:
                raise RuntimeError("请先确认关注专题")
            task_id = submit(client, "/questions", {"target_kind": "topic", "target_id": topic["id"], "purpose": "行业竞争", "main_question": "当前专题的关键性能与模型支持说法，哪些仍缺少原始资料或独立验证？", "subquestion_1": "各项陈述来自厂商还是独立来源？", "subquestion_2": "测试条件和软件版本是否足够清楚？", "completion": "列出有来源的结论、适用边界和证据缺口；缺少证据时停止推断。"})
            wait_task(task_id)
            result = station.decode(db.one("SELECT result_json FROM tasks WHERE id=?", (task_id,))["result_json"])
            assert result["document_ids"]
            print({"complete": result["complete"], "sources": len(result["document_ids"]), "uncertainties": len(result["uncertainties"])}, flush=True)
        elif arguments.stage == "schedule":
            original = station.status()
            zone = ZoneInfo(settings()["limits"]["timezone"])
            clock = (datetime.now(zone) + timedelta(minutes=1)).strftime("%H:%M")
            try:
                response = client.post(URL + "/watchlist/automation", data={"csrf": client.cookies["researcher_csrf"], "enabled": "yes", "schedule_time": clock})
                assert response.status_code == 303, response.text
                due = station.status()["next_run"]
                if not due:
                    raise RuntimeError("请先启用至少一个信源")
                deadline = time.monotonic() + 100
                while time.monotonic() < deadline:
                    if db.one("SELECT day FROM daily_briefings WHERE created_at>=? LIMIT 1", (due,)) and station.status()["next_run"] > due:
                        break
                    time.sleep(1)
                else:
                    raise RuntimeError("规则日报未在到期后生成")
            finally:
                restore = {"csrf": client.cookies["researcher_csrf"], "schedule_time": original["schedule_time"]}
                if original["auto_enabled"]:
                    restore["enabled"] = "yes"
                assert client.post(URL + "/watchlist/automation", data=restore).status_code == 303
        elif arguments.stage == "cancel":
            task_id = submit(client, "/settings/test", {"profile_id": get_profile().id, "mode": "test_chat"})
            assert client.post(URL + f"/tasks/{task_id}/cancel", data={"csrf": client.cookies["researcher_csrf"]}).status_code == 303
            for _ in range(30):
                if db.one("SELECT status FROM tasks WHERE id=?", (task_id,))["status"] == "cancelled":
                    break
                time.sleep(.2)
            response = client.get(URL + f"/tasks/{task_id}/events")
            assert response.status_code == 200 and "cancelled" in response.text
        else:
            if db.one("SELECT id FROM tasks WHERE status IN ('queued','running') LIMIT 1"):
                raise RuntimeError("存在执行中的任务，请等待完成")
            original = settings()["limits"]
            before = usage_summary()["requests"]
            if before < 1:
                raise RuntimeError("需要已有真实调用记录才能验证额度停止行为")
            changed = {**original, "daily_requests": before}
            try:
                assert client.post(URL + "/settings/limits", data={"csrf": client.cookies["researcher_csrf"], **changed}).status_code == 303
                task_id = submit(client, "/settings/test", {"profile_id": get_profile().id, "mode": "test_chat"})
                for _ in range(30):
                    task = db.one("SELECT status,error FROM tasks WHERE id=?", (task_id,))
                    if task["status"] == "failed":
                        break
                    time.sleep(.2)
                assert task["status"] == "failed" and "每日请求次数上限" in task["error"]
                assert usage_summary()["requests"] == before
            finally:
                if settings()["limits"] == changed:
                    assert client.post(URL + "/settings/limits", data={"csrf": client.cookies["researcher_csrf"], **original}).status_code == 303
    print(f"阶段 {arguments.stage} 的真实验证已通过。", flush=True)


if __name__ == "__main__":
    main()
