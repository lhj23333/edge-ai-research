from . import db
from .config import fingerprint, get_profile, settings

STATES = {"queued": "排队中", "running": "正在研究", "success": "已完成", "failed": "执行失败", "cancelled": "已取消", "interrupted": "已中断"}
KINDS = {"research": "历史联网研究", "chat": "历史讨论", "summary": "整理成果", "test_chat": "连接测试", "test_search": "联网测试", "onboard": "提出关注名单", "scan": "历史行业检查", "feed": "受控信源采集", "event_research": "事件深度核验", "selectbench": "SelectBench 评测", "question": "定向研究", "revise": "根据已有证据修订专题", "interactive": "探索研究", "followup": "后续问题研究"}
KINDS["test_tools"] = "研究工具调用测试"


def enqueue_test(kind, profile_id):
    profile = get_profile(profile_id)
    if kind == "test_search" and profile.provider == "compatible":
        raise ValueError("该配置未提供独立联网搜索")
    return db.execute("INSERT INTO tasks(kind,profile_id,profile_fingerprint,provider,model,created_at,priority) VALUES(?,?,?,?,?,?,10)",
                      (kind, profile.id, fingerprint(profile), profile.provider, profile.model, db.now()))


def task_documents(task_id):
    return db.query("SELECT d.id,d.title,d.url,d.publisher,d.published_at,v.id AS version_id,v.extent,v.fetched_at,v.fetched_at AS last_fetched_at FROM document_task_links l JOIN documents d ON d.id=l.document_id "
                    "JOIN document_versions v ON v.id=coalesce(l.recorded_version_id,(SELECT id FROM document_versions WHERE document_id=d.id "
                    "ORDER BY CASE WHEN extent LIKE '%原文%' THEN 0 ELSE 1 END,id DESC LIMIT 1)) "
                    "WHERE l.task_id=? ORDER BY d.id", (task_id,))


def save_artifact(topic_id, title, body, origin_task=None):
    with db.transaction() as connection:
        previous = connection.execute("SELECT id,version,title,body FROM artifacts WHERE topic_id=? ORDER BY id DESC LIMIT 1", (topic_id,)).fetchone()
        if previous and previous["title"] == title and previous["body"] == body:
            return previous["id"]
        cursor = connection.execute("INSERT INTO artifacts(topic_id,title,body,version,parent_id,origin_task,created_at) VALUES(?,?,?,?,?,?,?)", (topic_id, title, body, previous["version"] + 1 if previous else 1, previous["id"] if previous else None, origin_task, db.now()))
        return cursor.lastrowid


def usage_summary(by_route=False):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    day = datetime.now(ZoneInfo(settings()["limits"]["timezone"])).date().isoformat()
    aggregates = "count(*) AS requests,coalesce(sum(input_tokens),0) AS input_tokens,coalesce(sum(output_tokens),0) AS output_tokens,sum(estimate) AS estimate,sum(CASE WHEN input_tokens IS NULL AND output_tokens IS NULL THEN 1 ELSE 0 END) AS unknown"
    if by_route:
        return db.query("SELECT route," + aggregates + " FROM usage WHERE day=? GROUP BY route ORDER BY route", (day,))
    return db.one("SELECT " + aggregates + " FROM usage WHERE day=?", (day,))
