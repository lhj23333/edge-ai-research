import sqlite3

from . import db
from .config import fingerprint, get_profile, settings

STATES = {"queued": "排队中", "running": "正在研究", "success": "已完成", "failed": "执行失败", "cancelled": "已取消", "interrupted": "已中断"}
KINDS = {"research": "历史联网研究", "chat": "历史讨论", "summary": "整理成果", "test_chat": "连接测试", "test_search": "联网测试", "onboard": "提出关注名单", "scan": "行业采集与认知更新", "question": "定向研究", "revise": "根据已有证据修订专题"}


def enqueue(topic_id, kind, prompt="", brief_id=None, profile_id=None, schedule_key=None):
    profile = get_profile(profile_id)
    if kind in ("research", "test_search") and profile.provider == "compatible":
        raise ValueError("该配置仅支持已有资料讨论，请选择其他服务商执行联网研究")
    if kind == "research" and not profile.search_verified:
        raise ValueError("请在设置中先完成该模型的联网测试")
    if kind in ("chat", "summary") and not profile.chat_verified:
        raise ValueError("请在设置中先完成该模型的连接测试")
    if topic_id and not db.one("SELECT id FROM topics WHERE id=?", (topic_id,)):
        raise ValueError("研究主题不存在")
    if brief_id and not db.one("SELECT id FROM briefs WHERE id=? AND topic_id=?", (brief_id, topic_id)):
        raise ValueError("简报与当前主题不匹配")
    try:
        with db.transaction() as connection:
            cursor = connection.execute("INSERT INTO tasks(topic_id,kind,prompt,profile_id,profile_fingerprint,provider,model,created_at,context_brief,schedule_key) VALUES(?,?,?,?,?,?,?,?,?,?)", (topic_id, kind, prompt, profile.id, fingerprint(profile), profile.provider, profile.model, db.now(), brief_id, schedule_key))
            task_id = cursor.lastrowid
            if kind == "chat":
                connection.execute("INSERT INTO messages(topic_id,task_id,brief_id,role,body,created_at) VALUES(?,?,?,?,?,?)", (topic_id, task_id, brief_id, "user", prompt, db.now()))
            return task_id
    except sqlite3.IntegrityError:
        if schedule_key:
            existing = db.one("SELECT id FROM tasks WHERE schedule_key=?", (schedule_key,))
            if existing:
                return existing["id"]
        raise ValueError("该主题已有排队或执行中的任务，请等待完成或取消任务") from None


def context(topic_id, brief_id=None):
    sections = []
    if brief_id:
        brief = db.one("SELECT * FROM briefs WHERE id=? AND topic_id=?", (brief_id, topic_id))
        if brief:
            sections.append(f"当前阅读的简报（{brief['created_at']}）：\n{brief['body'][:12000]}")
    for artifact in db.query("SELECT * FROM artifacts WHERE topic_id=? ORDER BY id DESC LIMIT 3", (topic_id,)):
        sections.append(f"已有研究成果，版本 {artifact['version']}（{artifact['created_at']}）：\n{artifact['body'][:8000]}")
    for brief in db.query("SELECT * FROM briefs WHERE topic_id=? ORDER BY id DESC LIMIT 2", (topic_id,)):
        if brief["id"] != brief_id:
            sections.append(f"此前简报（{brief['created_at']}）：\n{brief['body'][:7000]}")
    messages = db.query("SELECT m.* FROM messages m JOIN tasks t ON t.id=m.task_id WHERE m.topic_id=? AND m.role IN ('user','assistant') AND t.status='success' ORDER BY m.id DESC LIMIT 12", (topic_id,))
    for message in reversed(messages):
        sections.append(f"{message['role']}（{message['created_at']}）：{message['body'][:3000]}")
    return "\n\n".join(sections)[:52000]


def save_artifact(topic_id, title, body, origin_task=None):
    with db.transaction() as connection:
        previous = connection.execute("SELECT id,version FROM artifacts WHERE topic_id=? ORDER BY id DESC LIMIT 1", (topic_id,)).fetchone()
        cursor = connection.execute("INSERT INTO artifacts(topic_id,title,body,version,parent_id,origin_task,created_at) VALUES(?,?,?,?,?,?,?)", (topic_id, title, body, previous["version"] + 1 if previous else 1, previous["id"] if previous else None, origin_task, db.now()))
        return cursor.lastrowid


def usage_summary():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    day = datetime.now(ZoneInfo(settings()["limits"]["timezone"])).date().isoformat()
    return db.one("SELECT count(*) AS requests,coalesce(sum(input_tokens),0) AS input_tokens,coalesce(sum(output_tokens),0) AS output_tokens,sum(estimate) AS estimate,sum(CASE WHEN input_tokens IS NULL AND output_tokens IS NULL THEN 1 ELSE 0 END) AS unknown FROM usage WHERE day=?", (day,))
