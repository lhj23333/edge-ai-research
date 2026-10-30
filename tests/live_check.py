import argparse
import subprocess
import time
from datetime import datetime, timedelta, timezone
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
    parser.add_argument("--stage", choices=["connection", "search", "research", "question", "revise", "schedule", "cancel", "quota"], default="research")
    parser.add_argument("--topic", type=int)
    parser.add_argument("--simulate-pause", action="store_true")
    arguments = parser.parse_args()
    profile = get_profile()
    with httpx.Client(trust_env=False, timeout=20) as client:
        client.get(URL + "/settings").raise_for_status()
        if arguments.stage in ("connection", "search"):
            task_id = submit(client, "/settings/test", {"profile_id": profile.id, "mode": "test_chat" if arguments.stage == "connection" else "test_search"})
            wait_task(task_id)
        elif arguments.stage == "research":
            task_id = submit(client, "/updates/scan", {})
            wait_task(task_id)
            run = db.one("SELECT id,status FROM collection_runs WHERE task_id=?", (task_id,))
            assert run and db.one("SELECT id FROM daily_issues WHERE run_id=?", (run["id"],))
            print({"collection_status": run["status"], "requests": db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (task_id,))["n"]}, flush=True)
        elif arguments.stage == "revise":
            topic = db.one("SELECT * FROM topics WHERE id=?", (arguments.topic,)) if arguments.topic else db.one("SELECT * FROM topics WHERE research_state='active' ORDER BY id LIMIT 1")
            if not topic:
                raise RuntimeError("请先确认关注专题")
            task_id = submit(client, f"/knowledge/{topic['id']}/revise", {})
            wait_task(task_id)
            result = station.decode(db.one("SELECT result_json FROM tasks WHERE id=?", (task_id,))["result_json"])
            print({"topic": topic["id"], "revisions": len(result["revision_ids"]), "questions": len(result["questions"])}, flush=True)
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
            if db.one("SELECT id FROM tasks WHERE status IN ('queued','running') LIMIT 1"):
                raise RuntimeError("存在执行中的任务，请等待完成")
            original = station.status()
            clock = (datetime.now(ZoneInfo("Asia/Shanghai")) + timedelta(minutes=1)).strftime("%H:%M")
            latest_id = db.one("SELECT coalesce(max(id),0) AS id FROM tasks")["id"]
            task_id = None
            try:
                response = client.post(URL + "/watchlist/automation", data={"csrf": client.cookies["researcher_csrf"], "enabled": "yes", "schedule_time": clock})
                assert response.status_code == 303
                due = datetime.fromisoformat(station.status()["next_run"])
                print(f"单次调度验证已设置：{clock}，没有打开浏览器。", flush=True)
                if arguments.simulate_pause:
                    subprocess.run(["systemctl", "--user", "stop", "researcher-worker"], check=True)
                    try:
                        while datetime.now(timezone.utc) <= due + timedelta(seconds=2):
                            time.sleep(1)
                    finally:
                        subprocess.run(["systemctl", "--user", "start", "researcher-worker"], check=True)
                deadline = time.monotonic() + 100
                while time.monotonic() < deadline:
                    scheduled = db.one("SELECT id FROM tasks WHERE id>? AND kind='scan' AND schedule_key IS NOT NULL ORDER BY id LIMIT 1", (latest_id,))
                    if scheduled:
                        task_id = scheduled["id"]
                        break
                    time.sleep(1)
                if not task_id:
                    raise RuntimeError("到期检查没有入队")
            finally:
                restore = {"csrf": client.cookies["researcher_csrf"], "schedule_time": original["schedule_time"]}
                if original["auto_enabled"]:
                    restore["enabled"] = "yes"
                assert client.post(URL + "/watchlist/automation", data=restore).status_code == 303
                print("更新设置已恢复。", flush=True)
            wait_task(task_id)
            assert db.one("SELECT count(*) AS n FROM tasks WHERE id>? AND schedule_key IS NOT NULL", (latest_id,))["n"] == 1
            print({"scheduled_task": task_id, "requests": db.one("SELECT count(*) AS n FROM usage WHERE task_id=?", (task_id,))["n"]}, flush=True)
        elif arguments.stage == "cancel":
            task_id = submit(client, "/settings/test", {"profile_id": profile.id, "mode": "test_chat"})
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
                task_id = submit(client, "/settings/test", {"profile_id": profile.id, "mode": "test_chat"})
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
