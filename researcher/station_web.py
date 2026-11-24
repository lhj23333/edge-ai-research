import re
from datetime import datetime, timedelta, timezone
from typing import Annotated
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from . import db, knowledge as evidence_store, research, station
from .config import settings
from .industry import selection
from .ingest import briefing, jobs
from .ingest.cluster import show_heat
from .ingest.collectors import stamp
from .providers import ResearchError, Source
from .scheduler import next_run, pause_topic_schedule
from .service import usage_summary
from .web_ids import OptionalRecordId, RecordId, parse_record_id

COMPARE_FIELDS = ("业务定位", "模型覆盖", "性能", "功耗成本", "软件迁移", "测试条件", "收费")
COMPARE_CONDITIONS = (
    r"(?:模型|负载|workload|model)[ :：=]*(?!典型|默认|未提供|未知|待确认|unknown|default)[\w][\w.+/-]+",
    r"(?<![a-z0-9])(?:u?int[248]|u?int16|fp(?:8|16|32)|bf16|tf32|w\d+a\d+)(?![a-z0-9])",
    r"(?:批量(?:大小)?|批次|batch(?:[ _-]*size)?)[ :：=]*[1-9]\d*(?![.\d])",
    r"(?:功耗(?:上限|限制)?|功率|tdp|power(?: limit)?)[ :：=]*\d+(?:\.\d+)?\s*(?:mw|w|瓦)(?![a-z])",
    r"(?:设备(?:环境)?|测试平台|硬件平台|主机|device|host|platform)[ :：=]*(?!典型|默认|未提供|未知|待确认|unknown|default)[\w][\w.+/-]+",
)
LABEL_ORDER = ("公开披露", "厂商宣称", "分析判断", "待核实")
CLAIM_CHUNK = """(SELECT ch.id FROM claim_evidence ce JOIN document_chunks ch ON ch.id=ce.chunk_id
                  WHERE ce.claim_id=c.id AND ch.version_id=c.document_version_id ORDER BY ch.position LIMIT 1) AS source_chunk_id"""


def rows_by_ids(table, ids):
    values = tuple(parse_record_id(value) for value in ids)
    if not values:
        return []
    return db.query(f"SELECT * FROM {table} WHERE id IN (" + ",".join("?" for _ in values) + ") ORDER BY id DESC", values)


def complete_test_conditions(value):
    return all(re.search(pattern, value) for pattern in COMPARE_CONDITIONS)


def event_topics(event_id, now, policy):
    return db.query("""
        WITH recent_reports AS (
            SELECT i.event_id,i.tier,coalesce(i.manual_score,i.score) AS score,
                   coalesce(json_extract(i.output_json,'$.event_type'),e.event_type) AS event_type,
                   julianday(coalesce(i.event_visible_at,i.updated_at)) AS visible_at
            FROM feed_items i JOIN events e ON e.id=i.event_id
            WHERE i.state='clustered' AND i.published_at>=:cutoff AND e.state='active' AND e.canonical_event_id IS NULL
        ), related_topics AS (
            SELECT t.id,t.title,t.research_state,et.impact,
                   coalesce((SELECT max(julianday(v.created_at)) FROM judgments j
                             JOIN judgment_versions v ON v.id=j.current_version_id WHERE j.topic_id=t.id),0) AS revised_at
            FROM event_topics et JOIN topics t ON t.id=et.topic_id WHERE et.event_id=:event_id
        )
        SELECT t.*,CASE WHEN t.research_state!='active' THEN NULL
            WHEN EXISTS (SELECT 1 FROM recent_reports r WHERE r.event_id=:event_id AND r.visible_at>t.revised_at AND r.tier='T1'
                         AND (r.event_type='产品发布' OR (r.score>=:high_score AND r.event_type IN (SELECT value FROM json_each(:major_types)))))
                THEN '已归档一手发布或重大更新的资料。'
            WHEN EXISTS (SELECT 1 FROM recent_reports r WHERE r.event_id=:event_id AND r.visible_at>t.revised_at AND r.score>=:high_score)
                 AND (SELECT count(DISTINCT r.event_id) FROM recent_reports r WHERE r.visible_at>t.revised_at AND r.score>=:high_score
                      AND (EXISTS (SELECT 1 FROM event_topics et WHERE et.topic_id=t.id AND et.event_id=r.event_id)
                           OR EXISTS (SELECT 1 FROM event_entities origin JOIN event_entities related USING(entity_id)
                                      JOIN entities x ON x.id=origin.entity_id AND x.kind='厂商'
                                      WHERE origin.event_id=:event_id AND related.event_id=r.event_id)))>=2
                THEN '近期已有多件相关高分事件待纳入判断。'
        END AS refresh_reason FROM related_topics t
    """, {"event_id": event_id, "cutoff": stamp(now - timedelta(days=policy.cluster_days)),
           "high_score": policy.auto_research_score, "major_types": station.encode(policy.auto_research_types)})


def annotation_location(note):
    table, column, path, label = {
        "event": ("events", "title", "events", "行业事件"),
        "entity": ("entities", "name", "entities", "厂商与产品"),
        "topic": ("topics", "title", "knowledge", "专题"),
        "brief": ("briefs", "title", "briefs", "历史简报"),
        "artifact": ("artifacts", "title", "artifacts", "研究成果"),
    }[note["target_kind"]]
    item = db.one(f"SELECT {column} AS title FROM {table} WHERE id=?", (note["target_id"],))
    return {"title": item["title"] if item else "原资料已不存在", "label": label, "path": f"/{path}/{note['target_id']}"}


def reading_snapshot(now):
    oldest = stamp(now - timedelta(hours=selection().freshness_hours))
    with db.open_database() as connection:
        connection.execute("BEGIN")
        state = dict(connection.execute("SELECT * FROM station_settings WHERE id=1").fetchone())
        latest = connection.execute("SELECT max(event_visible_at) FROM feed_items WHERE state='clustered'").fetchone()[0]
        rows = connection.execute("SELECT e.id,e.title,max(i.event_visible_at) AS visible_at FROM feed_items i JOIN events e ON e.id=i.event_id "
                                  "WHERE e.state='active' AND i.state='clustered' AND i.event_visible_at>? AND i.published_at>=? "
                                  "GROUP BY e.id ORDER BY visible_at DESC,e.id DESC", (state["last_feed_visible_at"] or "", oldest)).fetchall() if state["last_visit_at"] else []
    return state, latest, [dict(row) for row in rows]


def home_context():
    now = datetime.now(timezone.utc)
    state, latest, catchup_events = reading_snapshot(now)
    issue = briefing.content(now)
    day, start, _ = briefing.day_bounds(now, settings()["limits"]["timezone"])
    cutoff = state["last_visit_at"]
    catchup_revisions = briefing.revisions_since(cutoff) if cutoff else []
    progress = db.query("SELECT t.id,t.title,t.question,j.purpose,v.conclusion,v.status,v.created_at FROM topics t LEFT JOIN judgments j ON j.topic_id=t.id LEFT JOIN judgment_versions v ON v.id=j.current_version_id WHERE t.research_state='active' ORDER BY t.id,j.purpose")
    questions = db.query("SELECT question FROM research_gaps WHERE status IN ('open','deferred') AND updated_at>=? ORDER BY priority DESC LIMIT 8", (stamp(start),))
    return {"station": state, "day": day, "events": issue["events"], "background_events": issue["background"], "revisions": issue["revisions"],
            "progress": progress, "next_questions": [row["question"] for row in questions], "catchup_events": catchup_events,
            "catchup_revisions": catchup_revisions, "reading_at": now.isoformat(timespec="microseconds"), "reading_feed_at": latest,
            "task_count": db.one("SELECT count(*) AS n FROM tasks WHERE status IN ('queued','running')")["n"], **dashboard_context()}


def dashboard_context():
    def total(sql):
        return db.one(sql)["n"]

    metrics = {
        "companies": total("SELECT count(*) AS n FROM entities WHERE kind='厂商' AND state!='archived'"),
        "followed": total("SELECT count(*) AS n FROM entities WHERE kind='厂商' AND followed=1 AND state='active'"),
        "products": total("SELECT count(*) AS n FROM entities WHERE kind='产品' AND state!='archived'"),
        "software": total("SELECT count(*) AS n FROM entities WHERE kind='软件生态' AND state!='archived'"),
        "events": total("SELECT count(*) AS n FROM events WHERE state='active'"),
        "documents": total("SELECT count(*) AS n FROM documents"),
        "claims": total("SELECT count(*) AS n FROM claims"),
        "verified": total("SELECT count(*) AS n FROM claims WHERE verification='人工核对原网页摘录'"),
        "topics": total("SELECT count(*) AS n FROM topics WHERE research_state='active'"),
        "judgments": total("SELECT count(*) AS n FROM judgments j JOIN topics t ON t.id=j.topic_id WHERE j.current_version_id IS NOT NULL AND t.research_state='active'"),
    }
    labels = {row["label"]: row["n"] for row in db.query("SELECT label,count(*) AS n FROM claims GROUP BY label")}
    today = datetime.now(ZoneInfo(settings()["limits"]["timezone"])).date()
    days = [(today - timedelta(days=offset)).isoformat() for offset in range(6, -1, -1)]
    requests = {row["day"]: row["n"] for row in db.query("SELECT day,count(*) AS n FROM usage WHERE day>=? GROUP BY day", (days[0],))}
    products = db.query(
        "SELECT e.id,e.name,(SELECT value FROM entity_fields f WHERE f.entity_id=e.id AND f.field_name='性能' ORDER BY f.id DESC LIMIT 1) AS performance,"
        "(SELECT count(DISTINCT field_name) FROM entity_fields f WHERE f.entity_id=e.id AND f.field_name IN (" + ",".join("?" for _ in COMPARE_FIELDS) + ")) AS dimensions "
        "FROM entities e WHERE e.kind='产品' AND e.state!='archived' ORDER BY dimensions DESC,e.name LIMIT 4",
        COMPARE_FIELDS,
    )
    return {
        "metrics": metrics,
        "evidence_mix": [{"label": label, "n": labels.get(label, 0)} for label in LABEL_ORDER],
        "event_mix": db.query("SELECT event_type AS label,count(*) AS n FROM events WHERE state='active' GROUP BY event_type ORDER BY n DESC,event_type"),
        "usage_days": [{"day": day, "n": requests.get(day, 0)} for day in days],
        "usage_today": usage_summary(),
        "products": products,
        "compare_query": urlencode([("id", item["id"]) for item in products[:3]]) if len(products) > 1 else "",
        "dimension_total": len(COMPARE_FIELDS),
        "recent_tasks": db.query("SELECT x.id,x.kind,x.status,x.created_at,t.title AS topic_title FROM tasks x LEFT JOIN topics t ON t.id=x.topic_id ORDER BY x.id DESC LIMIT 5"),
    }


def register(app, page, form_data, redirect, required, text):
    router = APIRouter()

    @router.post("/reading/visit")
    def reading_visit(form=Depends(form_data)):
        now = datetime.now(timezone.utc)
        try:
            visit_at, feed_at = (datetime.fromisoformat(value).astimezone(timezone.utc) if value else None
                                 for value in (str(form.get("reading_at", "")), str(form.get("reading_feed_at", ""))))
        except ValueError:
            return JSONResponse({"error": "阅读进度时间无效"}, status_code=400)
        if not visit_at or visit_at > now or (feed_at and feed_at > now):
            return JSONResponse({"error": "阅读进度时间无效"}, status_code=400)
        db.execute("UPDATE station_settings SET last_visit_at=max(coalesce(last_visit_at,''),?),"
                   "last_feed_visible_at=nullif(max(coalesce(last_feed_visible_at,''),coalesce(?,'')),'') WHERE id=1",
                   (stamp(visit_at), feed_at.isoformat(timespec="microseconds") if feed_at else None))
        return JSONResponse({"status": "saved"})

    @router.post("/tasks/{task_id}/retry")
    def retry_task(task_id: RecordId, form=Depends(form_data)):
        try:
            research.retry(task_id)
            return redirect(f"/tasks/{task_id}", "任务将继续使用原有调用额度")
        except ValueError as error:
            return redirect(f"/tasks/{task_id}", error=str(error))

    @router.get("/watchlist", response_class=HTMLResponse)
    def watchlist(request: Request):
        return page(request, "watchlist.html", title="关注与更新设置", station=station.status(), proposals=db.query("SELECT p.*,d.title AS document_title,d.url AS document_url FROM watchlist_proposals p LEFT JOIN documents d ON d.id=p.document_id ORDER BY p.kind,p.id"), entities=db.query("SELECT * FROM entities WHERE followed=1 ORDER BY kind,name"), topics=db.query("SELECT * FROM topics WHERE research_state='active' ORDER BY id"))

    @router.post("/watchlist/propose")
    def propose(form=Depends(form_data)):
        try:
            return redirect(f"/tasks/{research.enqueue('onboard')}")
        except ValueError as error:
            return redirect("/watchlist", error=str(error))

    @router.post("/watchlist/confirm")
    def confirm_watchlist(form=Depends(form_data)):
        try:
            selected = {parse_record_id(value) for value in form.getlist("proposal_id")}
            if not selected and not db.one("SELECT id FROM entities WHERE followed=1 AND state='active' LIMIT 1") and not db.one("SELECT id FROM topics WHERE research_state='active' LIMIT 1"):
                raise ValueError("请至少选择一项厂商或专题")
            for proposal in db.query("SELECT * FROM watchlist_proposals WHERE status='accepted'"):
                if proposal["id"] in selected:
                    continue
                if proposal["kind"] == "company":
                    db.execute("UPDATE entities SET followed=0 WHERE kind='厂商' AND normalized_name=?", (station.normalized(proposal["name"]),))
                else:
                    with db.transaction() as connection:
                        topics = connection.execute("UPDATE topics SET research_state='archived',schedule='manual',next_run=NULL WHERE title=? RETURNING id", (proposal["name"],)).fetchall()
                        for topic in topics:
                            pause_topic_schedule(connection, topic["id"])
                db.execute("UPDATE watchlist_proposals SET status='declined' WHERE id=?", (proposal["id"],))
            for proposal in rows_by_ids("watchlist_proposals", selected):
                if proposal["kind"] == "company":
                    entity_id = station.entity("厂商", proposal["name"])
                    db.execute("UPDATE entities SET followed=1,state='active' WHERE id=?", (entity_id,))
                elif proposal["kind"] == "topic":
                    question = proposal["reason"].split("\n", 1)[0]
                    current = db.one("SELECT id FROM topics WHERE title=?", (proposal["name"],))
                    if current:
                        db.execute("UPDATE topics SET research_state='active' WHERE id=?", (current["id"],))
                    else:
                        db.execute("INSERT INTO topics(title,question,kind,created_at,research_state) VALUES(?,?, '专题',?,'active')", (proposal["name"], question, db.now()))
                db.execute("UPDATE watchlist_proposals SET status='accepted' WHERE id=?", (proposal["id"],))
            db.execute("UPDATE station_settings SET watchlist_confirmed=1 WHERE id=1")
            return redirect("/watchlist", "关注名单已更新")
        except (ValueError, TypeError) as error:
            return redirect("/watchlist", error=str(error))

    @router.post("/watchlist/add")
    def add_watch(form=Depends(form_data)):
        try:
            kind = text(form, "kind", 20)
            name = text(form, "name", 160)
            if kind == "厂商":
                identifier = station.entity("厂商", name)
                db.execute("UPDATE entities SET followed=1,state='active' WHERE id=?", (identifier,))
            elif kind == "专题":
                question = text(form, "question", 1000)
                existing = db.one("SELECT id FROM topics WHERE title=? ORDER BY id LIMIT 1", (name,))
                if existing:
                    identifier = existing["id"]
                    db.execute("UPDATE topics SET question=?,research_state='active' WHERE id=?", (question, identifier))
                else:
                    identifier = db.execute("INSERT INTO topics(title,question,kind,created_at,research_state) VALUES(?,?,'专题',?,'active')", (name, question, db.now()))
            else:
                raise ValueError("仅可添加厂商或专题")
            return redirect("/watchlist", "关注对象已加入")
        except ValueError as error:
            return redirect("/watchlist", error=str(error))

    @router.post("/watchlist/automation")
    def automation(form=Depends(form_data)):
        try:
            enabled = str(form.get("enabled")) == "yes"
            if enabled and not db.one("SELECT id FROM feed_sources WHERE enabled=1 LIMIT 1"):
                raise ValueError("请先在信源管理中启用信源")
            clock = text(form, "schedule_time", 5)
            datetime.strptime(clock, "%H:%M")
            upcoming = next_run(clock, settings()["limits"]["timezone"]) if enabled else None
            db.execute("UPDATE station_settings SET auto_enabled=?,schedule_time=?,next_run=?,sources_confirmed=max(sources_confirmed,?) WHERE id=1", (int(enabled), clock, upcoming, int(enabled)))
            return redirect("/watchlist", "自动更新设置已保存")
        except ValueError as error:
            return redirect("/watchlist", error=str(error))

    @router.post("/updates/collect")
    def collect_updates(form=Depends(form_data)):
        try:
            return redirect(f"/tasks/{jobs.enqueue()}")
        except ValueError as error:
            return redirect("/updates", error=str(error))

    @router.get("/updates", response_class=HTMLResponse)
    def updates(request: Request, view: str = "selected", event_type: str = "", company: str = "",
                topic_id: OptionalRecordId = None, after: str = "", before: str = "", q: str = ""):
        policy = selection()
        now = datetime.now(timezone.utc)
        view = view if view in {"selected", "all", "heat"} else "selected"
        filters = ["e.state='active'", "e.canonical_event_id IS NULL"]
        args = []
        if view == "selected":
            filters.append("f.event_id IS NOT NULL")
        elif view == "heat":
            filters.append("f.latest_publication>=?")
            args.append(stamp(now - timedelta(hours=policy.freshness_hours)))
        else:
            filters.append("(f.feed_score>=? OR e.identity_key NOT LIKE 'feed:%')")
            args.append(policy.browse_floor)
        if event_type:
            filters.append("e.event_type=?")
            args.append(event_type)
        if company:
            filters.append("EXISTS(SELECT 1 FROM event_entities ee JOIN entities x ON x.id=ee.entity_id WHERE ee.event_id=e.id AND x.name LIKE ?)")
            args.append("%" + company[:100] + "%")
        if topic_id:
            filters.append("EXISTS(SELECT 1 FROM event_topics WHERE event_id=e.id AND topic_id=?)")
            args.append(topic_id)
        if after:
            filters.append("coalesce(f.latest_publication,e.published_at,e.occurred_at,e.created_at)>=?")
            args.append(after[:10])
        if before:
            filters.append("substr(coalesce(f.latest_publication,e.published_at,e.occurred_at,e.created_at),1,10)<=?")
            args.append(before[:10])
        if q:
            filters.append("(e.title LIKE ? OR e.what_happened LIKE ?)")
            args.extend(["%" + q[:100] + "%"] * 2)
        order = "e.heat_score DESC,f.feed_score DESC,e.id DESC" if view == "heat" else "coalesce(f.latest_publication,e.published_at,e.created_at) DESC,e.id DESC"
        events = db.query(briefing.EVENT_FEEDS + "SELECT e.*,f.feed_score,f.latest_publication,(SELECT count(*) FROM event_claims WHERE event_id=e.id) AS evidence_count "
                          "FROM events e LEFT JOIN feed_stats f ON f.event_id=e.id WHERE " + " AND ".join(filters) + " ORDER BY " + order + " LIMIT 150", tuple(args))
        show_heat(events, now, policy)
        if view == "heat":
            events.sort(key=lambda row: (row["heat_score"], row["feed_score"], row["id"]), reverse=True)
        pending = []
        if view == "all" and not (event_type or topic_id):
            candidate_filters = ["i.state='scored_out'", "i.event_id IS NULL", "coalesce(i.manual_score,i.score)>=?", "i.title LIKE ?"]
            candidate_params = [policy.browse_floor, "%" + q[:100] + "%"]
            if after:
                candidate_filters.append("i.published_at>=?")
                candidate_params.append(after[:10])
            if before:
                candidate_filters.append("substr(i.published_at,1,10)<=?")
                candidate_params.append(before[:10])
            if company:
                candidate_filters.append("(x.name LIKE ? OR i.title LIKE ?)")
                candidate_params.extend(["%" + company[:100] + "%"] * 2)
            pending = db.query("SELECT i.*,coalesce(i.manual_score,i.score) AS feed_score FROM feed_items i LEFT JOIN entities x ON x.id=i.owner_entity_id "
                               "WHERE " + " AND ".join(candidate_filters) + " ORDER BY i.published_at DESC LIMIT 80", tuple(candidate_params))
        controls = {"view": view, "event_type": event_type, "company": company, "topic_id": topic_id, "after": after, "before": before, "q": q}
        return page(request, "updates.html", title="行业动态", events=events, candidates=pending, station=station.status(), filters=controls,
                    event_types=policy.event_types, browse_floor=policy.browse_floor,
                    topics=db.query("SELECT id,title FROM topics WHERE research_state='active' ORDER BY title"),
                    runs=db.query("SELECT r.*,s.name FROM feed_runs r JOIN feed_sources s ON s.id=r.source_id ORDER BY r.id DESC LIMIT 8"))

    @router.post("/updates/manual")
    def manual_event(form=Depends(form_data)):
        try:
            title = text(form, "title", 180)
            what = text(form, "what_happened", 700)
            why = text(form, "why_important", 700)
            company = text(form, "company", 120)
            event_type = text(form, "event_type", 30)
            if event_type not in selection().event_types:
                raise ValueError("事件类别无效")
            url = text(form, "url", 1000)
            excerpt = text(form, "excerpt", 2000)
            occurred = str(form.get("occurred_at") or "").strip()
            published = str(form.get("published_at") or "").strip()
            for value in (occurred, published):
                if value:
                    datetime.strptime(value, "%Y-%m-%d")
            source = Source(title, url, excerpt, "用户提供摘录", published or None)
            document_id, version_id = station.save_document(source, None)
            claim_id = station.save_claim(document_id, version_id, what, excerpt, "待核实", manual_excerpt=True)
            entity_id = station.entity("厂商", company)
            item = {"identity_key": f"{company}|{title}|{occurred}", "event_type": event_type, "title": title, "what_happened": what, "why_important": why, "competitive_impact": str(form.get("competitive_impact") or "")[:700], "occurred_at": occurred or None, "importance": 1, "background": True}
            event_id, _ = station.add_event(item, [claim_id], [entity_id], [])
            return redirect(f"/events/{event_id}", "人工补充已保存，资料仍待核验")
        except (ValueError, TypeError, ResearchError) as error:
            return redirect("/updates", error=str(error))

    @router.get("/events/{event_id}", response_class=HTMLResponse)
    def event_page(request: Request, event_id: RecordId, timeline: str = "asc"):
        event = required("SELECT * FROM events WHERE id=?", (event_id,))
        if event["canonical_event_id"]:
            return redirect(f"/events/{event['canonical_event_id']}", "相同公告的来源已合并到此事件")
        claims = db.query(f"SELECT c.*,d.title AS document_title,d.url,d.published_at,d.last_fetched_at,d.extent,{CLAIM_CHUNK} FROM event_claims ec JOIN claims c ON c.id=ec.claim_id JOIN documents d ON d.id=c.document_id WHERE ec.event_id=? ORDER BY c.id", (event_id,))
        entities = db.query("SELECT x.* FROM event_entities ee JOIN entities x ON x.id=ee.entity_id WHERE ee.event_id=?", (event_id,))
        now, policy = datetime.now(timezone.utc), selection()
        topics = event_topics(event_id, now, policy)
        revisions = db.query("SELECT v.*,j.purpose,t.title AS topic_title,j.topic_id FROM judgment_versions v JOIN judgments j ON j.id=v.judgment_id JOIN topics t ON t.id=j.topic_id WHERE v.event_id=? OR v.event_id IN (SELECT id FROM events WHERE canonical_event_id=?) ORDER BY v.id DESC", (event_id, event_id))
        materials = evidence_store.event_documents(event_id)
        show_heat([event], now, policy)
        return page(request, "event.html", title=event["title"], event=event, claims=claims, entities=entities, topics=topics, revisions=revisions,
                    materials=list(reversed(materials)) if timeline == "desc" else materials, timeline=timeline,
                    latest_research=db.one("SELECT id,session_id,status FROM tasks WHERE kind='event_research' AND json_extract(job_json,'$.target_id')=? ORDER BY id DESC LIMIT 1", (event_id,)))

    @router.post("/events/{event_id}/research")
    def research_event(event_id: RecordId, form=Depends(form_data)):
        required("SELECT id FROM events WHERE id=?", (event_id,))
        try:
            task_id = research.enqueue_event(event_id)
            task = db.one("SELECT session_id FROM tasks WHERE id=?", (task_id,))
            return redirect(f"/research/{task['session_id']}")
        except ValueError as error:
            return redirect(f"/events/{event_id}", error=str(error))

    @router.get("/evidence", response_class=HTMLResponse)
    def evidence(request: Request, q: str = "", label: str = ""):
        args = []
        where = []
        if q:
            where.append("(c.statement LIKE ? OR d.title LIKE ? OR d.url LIKE ?)")
            args.extend(["%" + q[:100] + "%"] * 3)
        search = ("WHERE " + " AND ".join(where) if where else "", tuple(args))
        if label:
            where.append("c.label=?")
            args.append(label)
        claims = db.query(f"SELECT c.*,d.title AS document_title,d.url,d.published_at,d.last_fetched_at,d.extent,{CLAIM_CHUNK} FROM claims c JOIN documents d ON d.id=c.document_id " + ("WHERE " + " AND ".join(where) if where else "") + " ORDER BY c.id DESC LIMIT 150", tuple(args))
        label_counts = {row["label"]: row["n"] for row in db.query("SELECT c.label,count(*) AS n FROM claims c JOIN documents d ON d.id=c.document_id " + search[0] + " GROUP BY c.label", search[1])}
        return page(request, "evidence.html", title="证据管理", claims=claims, documents=db.query("SELECT * FROM documents ORDER BY id DESC LIMIT 30"), q=q, label=label, labels=LABEL_ORDER, label_counts=label_counts)

    @router.get("/documents/{document_id}", response_class=HTMLResponse)
    def document_page(request: Request, document_id: RecordId, version: OptionalRecordId = None):
        document = required("SELECT * FROM documents WHERE id=?", (document_id,))
        selected = required("SELECT * FROM document_versions WHERE id=? AND document_id=?", (version, document_id)) if version else db.one("SELECT * FROM document_versions WHERE document_id=? ORDER BY CASE WHEN extent LIKE '%原文%' THEN 0 ELSE 1 END,id DESC LIMIT 1", (document_id,))
        if selected:
            document.update(content=selected["content"], extent=selected["extent"], verification=selected["verification"], last_fetched_at=selected["fetched_at"])
        versions = db.query("SELECT id,extent,fetched_at,verification,content_hash FROM document_versions WHERE document_id=? ORDER BY id DESC", (document_id,))
        claims = db.query(f"SELECT c.*,d.title AS document_title,d.url,d.published_at,d.last_fetched_at,d.extent,{CLAIM_CHUNK} FROM claims c JOIN documents d ON d.id=c.document_id WHERE c.document_id=? ORDER BY c.id DESC", (document_id,))
        chunks = db.query("SELECT * FROM document_chunks WHERE version_id=? ORDER BY position", (selected["id"],)) if selected else []
        checks = db.query("SELECT ce.*,c.statement,dc.version_id FROM claim_evidence ce JOIN claims c ON c.id=ce.claim_id JOIN document_chunks dc ON dc.id=ce.chunk_id WHERE c.document_id=? ORDER BY ce.created_at DESC", (document_id,))
        snapshots = db.query("SELECT id,version_id,content_type,method,length(body) AS bytes,created_at FROM source_snapshots WHERE version_id IN (SELECT id FROM document_versions WHERE document_id=?) ORDER BY id DESC", (document_id,))
        return page(request, "document.html", title=document["title"], document=document, versions=versions, claims=claims, chunks=chunks, selected_version=selected["id"] if selected else None, evidence_checks=checks, snapshots=snapshots)

    @router.get("/documents/{document_id}/snapshots/{snapshot_id}")
    def download_snapshot(document_id: RecordId, snapshot_id: RecordId):
        snapshot = required("SELECT s.* FROM source_snapshots s JOIN document_versions v ON v.id=s.version_id WHERE s.id=? AND v.document_id=?", (snapshot_id, document_id))
        extension = "pdf" if "pdf" in snapshot["content_type"] else "html" if "html" in snapshot["content_type"] else "json" if "json" in snapshot["content_type"] else "txt"
        return Response(snapshot["body"], media_type=snapshot["content_type"], headers={"Content-Disposition": f'attachment; filename="source-{document_id}-v{snapshot["version_id"]}.{extension}"'})

    @router.post("/claims/{claim_id}/verify")
    def verify_claim(claim_id: RecordId, form=Depends(form_data)):
        claim = required("SELECT * FROM claims WHERE id=?", (claim_id,))
        choice = str(form.get("verification"))
        if choice not in {"人工核对原网页摘录", "人工核对发现不符", "待核实"}:
            return redirect(f"/documents/{claim['document_id']}", error="核验状态无效")
        verified_at = db.now() if choice != "待核实" else None
        db.execute("UPDATE claims SET verification=?,verified_at=? WHERE id=?", (choice, verified_at, claim_id))
        if choice == "人工核对发现不符":
            from .knowledge import invalidate
            invalidate(claim["document_id"], "人工核对发现原文与既有说法不符")
        if choice == "人工核对原网页摘录":
            db.execute("UPDATE documents SET verification='至少一条摘录已人工核对',verified_at=? WHERE id=?", (verified_at, claim["document_id"]))
            db.execute("UPDATE document_versions SET verification='至少一条摘录已人工核对',verified_at=? WHERE id=?", (verified_at, claim["document_version_id"]))
        if choice == "人工核对发现不符":
            affected = db.query("SELECT j.topic_id,j.purpose,v.* FROM judgments j JOIN judgment_versions v ON v.id=j.current_version_id JOIN judgment_evidence je ON je.version_id=v.id WHERE je.claim_id=? AND je.stance='支持'", (claim_id,))
            for item in affected:
                support = [row["claim_id"] for row in db.query("SELECT claim_id FROM judgment_evidence WHERE version_id=? AND stance='支持' AND claim_id!=?", (item["id"], claim_id))]
                opposing = [row["claim_id"] for row in db.query("SELECT claim_id FROM judgment_evidence WHERE version_id=? AND stance='反对'", (item["id"],))] + [claim_id]
                station.revise(item["topic_id"], item["purpose"], item["conclusion"], "存在争议", item["boundaries"], (item["unresolved"] + "；原依据与原网页不符").strip("；"), "人工核验发现原有支持证据与原网页不符", item["event_id"], support, opposing)
        return redirect(f"/documents/{claim['document_id']}", "核验状态已更新；受影响的专题判断已记录修订" if choice == "人工核对发现不符" else "核验状态已更新")

    @router.get("/companies", response_class=HTMLResponse)
    def companies(request: Request, q: str = "", kind: str = ""):
        args = ["%" + q[:100] + "%"]
        condition = "name LIKE ? AND state!='archived'"
        if kind:
            condition += " AND kind=?"
            args.append(kind)
        entities = db.query("SELECT e.*,(SELECT count(*) FROM entity_fields WHERE entity_id=e.id) AS field_count,(SELECT group_concat(DISTINCT field_name) FROM entity_fields WHERE entity_id=e.id) AS field_names FROM entities e WHERE " + condition + " ORDER BY e.followed DESC,e.kind,e.name LIMIT 200", tuple(args))
        kind_counts = {row["kind"]: row["n"] for row in db.query("SELECT kind,count(*) AS n FROM entities WHERE name LIKE ? AND state!='archived' GROUP BY kind", (args[0],))}
        return page(request, "companies.html", title="厂商与产品", entities=entities, q=q, kind=kind, kinds_order=("厂商", "产品", "应用场景", "技术路线", "软件生态"), kind_counts=kind_counts)

    @router.get("/entities/{entity_id}", response_class=HTMLResponse)
    def entity_page(request: Request, entity_id: RecordId):
        item = required("SELECT * FROM entities WHERE id=?", (entity_id,))
        fields = db.query("SELECT f.*,c.label,c.verification,d.url,d.title AS document_title FROM entity_fields f LEFT JOIN claims c ON c.id=f.claim_id LEFT JOIN documents d ON d.id=c.document_id WHERE f.entity_id=? ORDER BY f.field_name,f.id DESC", (entity_id,))
        relations = db.query("SELECT r.*,a.name AS source_name,b.name AS target_name,CASE WHEN r.from_entity_id=? THEN b.id ELSE a.id END AS other_id,c.label,d.url FROM relations r JOIN entities a ON a.id=r.from_entity_id JOIN entities b ON b.id=r.to_entity_id LEFT JOIN claims c ON c.id=r.claim_id LEFT JOIN documents d ON d.id=c.document_id WHERE r.from_entity_id=? OR r.to_entity_id=? ORDER BY r.id DESC", (entity_id, entity_id, entity_id))
        events = db.query("SELECT e.* FROM event_entities ee JOIN events e ON e.id=ee.event_id WHERE ee.entity_id=? AND e.state='active' ORDER BY e.id DESC LIMIT 20", (entity_id,))
        products = db.query("SELECT * FROM entities WHERE parent_id=? AND state!='archived' ORDER BY name", (entity_id,))
        official_sources = db.query("SELECT id,name,url,enabled FROM feed_sources WHERE tier='T1' AND owner_entity_id=? ORDER BY enabled DESC,name", (item["parent_id"] or entity_id,))
        return page(request, "entity.html", title=item["name"], item=item, fields=fields, relations=relations, events=events, products=products, dimensions=COMPARE_FIELDS, official_sources=official_sources)

    @router.post("/entities/{entity_id}/follow")
    def follow_entity(entity_id: RecordId, form=Depends(form_data)):
        item = required("SELECT * FROM entities WHERE id=?", (entity_id,))
        value = str(form.get("follow")) == "yes"
        db.execute("UPDATE entities SET followed=?,state=?,updated_at=? WHERE id=?", (int(value), "active" if value else item["state"], db.now(), entity_id))
        return redirect(f"/entities/{entity_id}", "关注状态已更新")

    @router.post("/entities/{entity_id}/archive")
    def archive_entity(entity_id: RecordId, form=Depends(form_data)):
        required("SELECT id FROM entities WHERE id=?", (entity_id,))
        db.execute("UPDATE entities SET state='archived',followed=0,updated_at=? WHERE id=?", (db.now(), entity_id))
        return redirect("/companies", "资料已归档，历史记录仍可查阅")

    @router.get("/compare", response_class=HTMLResponse)
    def compare(request: Request, ids: Annotated[list[RecordId] | None, Query(alias="id")] = None):
        products = db.query("SELECT * FROM entities WHERE kind='产品' AND state!='archived' ORDER BY name")
        requested = set(ids or [])
        selected = sorted((product for product in products if product["id"] in requested), key=lambda product: product["id"], reverse=True)
        if len(selected) > 3:
            return page(request, "compare.html", title="产品对比", products=products, selected=[], rows=[], comparable=False, error_message="一次最多选择三款产品")
        selected_ids = tuple(product["id"] for product in selected)
        fields = db.query("SELECT f.*,c.label,d.url FROM entity_fields f LEFT JOIN claims c ON c.id=f.claim_id LEFT JOIN documents d ON d.id=c.document_id WHERE f.entity_id IN (" + ",".join("?" for _ in selected_ids) + ") ORDER BY f.updated_at,f.id", selected_ids) if selected_ids else []
        by_product = {(item["entity_id"], item["field_name"]): item for item in fields}
        rows = [{"name": name, "values": [by_product.get((product["id"], name)) for product in selected]} for name in COMPARE_FIELDS]
        conditions = [station.normalized(by_product[(product["id"], "测试条件")]["value"]) for product in selected if (product["id"], "测试条件") in by_product]
        comparable = len(selected) > 1 and len(conditions) == len(selected) and len(set(conditions)) == 1 and complete_test_conditions(conditions[0])
        return page(request, "compare.html", title="产品对比", products=products, selected=selected, rows=rows, comparable=comparable, error_message="")

    @router.get("/map", response_class=HTMLResponse)
    def knowledge_map(request: Request):
        entities = db.query("SELECT * FROM entities WHERE state!='archived' ORDER BY kind,name LIMIT 100")
        relations = db.query("SELECT r.*,a.name AS source_name,b.name AS target_name FROM relations r JOIN entities a ON a.id=r.from_entity_id JOIN entities b ON b.id=r.to_entity_id ORDER BY r.id DESC LIMIT 150")
        topics = db.query("SELECT t.*,(SELECT count(*) FROM judgments j WHERE j.topic_id=t.id AND j.current_version_id IS NOT NULL) AS judgment_count FROM topics t WHERE t.research_state='active' ORDER BY t.id")
        return page(request, "map.html", title="认知地图", entities=entities, relations=relations, topics=topics)

    @router.get("/map/data")
    def map_data():
        entities = db.query("SELECT id,kind,name,summary FROM entities WHERE state!='archived' ORDER BY kind,name LIMIT 100")
        relations = db.query("SELECT id,from_entity_id,to_entity_id,relation_type,explanation FROM relations ORDER BY id DESC LIMIT 150")
        return {"entities": entities, "relations": relations}

    @router.get("/knowledge/{topic_id}", response_class=HTMLResponse)
    def knowledge(request: Request, topic_id: RecordId):
        topic = required("SELECT * FROM topics WHERE id=?", (topic_id,))
        judgments = db.query("SELECT j.*,v.conclusion,v.boundaries,v.unresolved,v.reason,v.created_at AS revised_at FROM judgments j LEFT JOIN judgment_versions v ON v.id=j.current_version_id WHERE j.topic_id=? ORDER BY j.purpose", (topic_id,))
        versions = db.query("SELECT v.*,j.purpose,(SELECT conclusion FROM judgment_versions prior WHERE prior.judgment_id=v.judgment_id AND prior.version=v.version-1) AS previous_conclusion FROM judgment_versions v JOIN judgments j ON j.id=v.judgment_id WHERE j.topic_id=? ORDER BY v.id DESC", (topic_id,))
        evidence = db.query(f"SELECT je.version_id,je.stance,j.current_version_id,j.purpose,c.*,d.title AS document_title,d.url,d.published_at,d.last_fetched_at,d.extent,{CLAIM_CHUNK} FROM judgment_evidence je JOIN judgment_versions v ON v.id=je.version_id JOIN judgments j ON j.id=v.judgment_id JOIN claims c ON c.id=je.claim_id JOIN documents d ON d.id=c.document_id WHERE j.topic_id=? ORDER BY je.version_id DESC", (topic_id,))
        events = db.query("SELECT e.*,et.impact FROM event_topics et JOIN events e ON e.id=et.event_id WHERE et.topic_id=? AND e.state='active' ORDER BY e.id DESC", (topic_id,))
        return page(request, "knowledge.html", title=topic["title"], topic=topic, judgments=judgments, versions=versions, evidence=evidence, current_evidence=[item for item in evidence if item["version_id"] == item["current_version_id"]], events=events)

    @router.post("/knowledge/{topic_id}/archive")
    def archive_topic(topic_id: RecordId, form=Depends(form_data)):
        required("SELECT id FROM topics WHERE id=?", (topic_id,))
        with db.transaction() as connection:
            connection.execute("UPDATE topics SET research_state='archived',schedule='manual',next_run=NULL WHERE id=?", (topic_id,))
            pause_topic_schedule(connection, topic_id)
        return redirect("/map", "专题已归档，判断历史仍可查阅")

    @router.post("/knowledge/{topic_id}/revise")
    def revise_knowledge(topic_id: RecordId, form=Depends(form_data)):
        topic = required("SELECT id,title,question,research_state FROM topics WHERE id=?", (topic_id,))
        path = f"/knowledge/{topic_id}"
        try:
            if topic["research_state"] == "archived":
                raise ValueError("已归档的专题不能刷新判断")
            scope = station.station_scope()
            scope["topics"] = [{key: topic[key] for key in ("id", "title", "question")}]
            job = {"topic_id": topic_id, "scope": scope}
            if form.get("event_id"):
                event_id = parse_record_id(form["event_id"])
                if not db.one("SELECT e.id FROM events e JOIN event_topics et ON et.event_id=e.id WHERE e.id=? AND et.topic_id=? AND e.state='active' AND e.canonical_event_id IS NULL", (event_id, topic_id)):
                    raise ValueError("该事件与当前专题没有有效关联")
                path = f"/events/{event_id}"
                job["trigger_event_id"] = event_id
            return redirect(f"/tasks/{research.enqueue('revise', job)}")
        except ValueError as error:
            return redirect(path, error=str(error))

    @router.get("/questions", response_class=HTMLResponse)
    def questions(request: Request, target_kind: str = "topic", target_id: OptionalRecordId = None, action: str = "",
                  event_id: OptionalRecordId = None, claim_id: OptionalRecordId = None):
        event = db.one("SELECT * FROM events WHERE id=?", (event_id,)) if event_id else None
        claim = db.one("SELECT * FROM claims WHERE id=?", (claim_id,)) if claim_id else None
        if claim and not event:
            event = db.one("SELECT e.* FROM events e JOIN event_claims ec ON ec.event_id=e.id WHERE ec.claim_id=? ORDER BY e.id DESC LIMIT 1", (claim_id,))
        if event and not target_id:
            related = db.one("SELECT entity_id FROM event_entities WHERE event_id=? LIMIT 1", (event["id"],))
            if related:
                target_kind, target_id = "entity", related["entity_id"]
        target = db.one("SELECT title AS name FROM topics WHERE id=?", (target_id,)) if target_kind == "topic" else db.one("SELECT name FROM entities WHERE id=?", (target_id,))
        name = target["name"] if target else "当前研究对象"
        defaults = {
            "verify": (f"{name}的关键说法是否得到原始公开资料支持？", ["原始来源与产品版本是什么？", "厂商陈述与独立证据是否一致？", "哪些部分仍无法核实？"], "给出可确认结论、原始来源、适用边界和证据缺口；证据不足时保留问题。", "行业竞争"),
            "explain": (f"{event['title'] if event else name + '近期变化'}会怎样影响行业竞争？", ["实际发生了什么变化？", "哪些厂商或应用受到影响？", "哪些既有专题判断需要修订？"], "解释事件、影响路径及需要修订的判断，区分证据与分析。", "行业竞争"),
            "compare": (f"{name}在指定场景下的模型覆盖、功耗成本与迁移限制是什么？", ["公开资料支持哪些模型与软件版本？", "性能和功耗测试条件是否完整？", "部署前还缺哪些验证？"], "列出部署条件、可核实能力与证据缺口；条件不一致时不排名。", "部署决策"),
            "update": (f"关于{name}的当前认识是否需要根据新证据修订？", ["哪些判断得到加强或削弱？", "是否存在反对证据？", "结论的适用边界是什么？"], "逐项说明保持或修订的结论、依据、原因与未解问题。", "行业竞争"),
            "gap": (f"{name}有哪些影响判断的关键证据缺口？", ["哪些信息尚未公开披露？", "哪些说法只来自厂商自述？", "下一项可公开验证的进展是什么？"], "形成按重要性排序的待核实问题，说明每项所需证据与停止条件。", "行业竞争"),
        }
        chosen = defaults.get(action, ("", ["", "", ""], "列出有来源的结论、适用条件和仍待核实的问题。", "行业竞争"))
        main = f"“{claim['statement'][:220]}”是否得到原始公开资料支持？" if claim and action == "verify" else chosen[0]
        prefill = {"main": main, "subquestions": chosen[1], "completion": chosen[2], "purpose": chosen[3]}
        notes = db.query("SELECT * FROM annotations WHERE archived=0 ORDER BY id DESC LIMIT 50")
        return page(request, "questions.html", title="定向研究", target_kind=target_kind, target_id=target_id, event_id=event["id"] if event else None, claim_id=claim_id if claim else None, action=action, prefill=prefill, topics=db.query("SELECT id,title FROM topics WHERE research_state='active' OR id=? ORDER BY title", (target_id if target_kind == "topic" else None,)), entities=db.query("SELECT id,name,kind FROM entities WHERE state!='archived' OR id=? ORDER BY kind,name", (target_id if target_kind == "entity" else None,)), notes=notes, questions=db.query("SELECT * FROM research_questions ORDER BY id DESC LIMIT 30"))

    @router.post("/questions")
    def create_question(form=Depends(form_data)):
        question_id = None
        try:
            target_kind = text(form, "target_kind", 20)
            if target_kind not in {"topic", "entity"} or not form.get("target_id"):
                raise ValueError("请选择具体的专题、厂商或产品")
            target_id = parse_record_id(form["target_id"])
            target_table = "topics" if target_kind == "topic" else "entities"
            required(f"SELECT id FROM {target_table} WHERE id=?", (target_id,))
            purpose = text(form, "purpose", 10)
            if purpose not in station.PURPOSES:
                raise ValueError("请选择研究用途")
            main = text(form, "main_question", 500)
            subquestions = [str(form.get(f"subquestion_{index}", "")).strip() for index in range(1, 4)]
            subquestions = [item[:300] for item in subquestions if item]
            completion = text(form, "completion", 500)
            conditions = {key: str(form.get(key, "")).strip()[:200] for key in ("scenario", "model", "hardware", "power", "software") if form.get(key)}
            research_context = {}
            for key, table in (("event_id", "events"), ("claim_id", "claims")):
                if form.get(key):
                    value = parse_record_id(form[key])
                    required(f"SELECT id FROM {table} WHERE id=?", (value,))
                    research_context[key] = value
            if research_context:
                conditions["research_context"] = research_context
            note_ids = [parse_record_id(value) for value in form.getlist("note_id")]
            for note_id in note_ids:
                if not db.one("SELECT id FROM annotations WHERE id=? AND archived=0", (note_id,)):
                    raise ValueError("选中的个人笔记不存在")
            job = {"target_kind": target_kind, "target_id": target_id, "purpose": purpose, "main_question": main, "subquestions": subquestions, "conditions": conditions, "completion": completion, "selected_note_ids": note_ids}
            needs_input = purpose == "部署决策" and not conditions.get("scenario")
            timestamp = db.now()
            question_id = db.execute("INSERT INTO research_questions(target_kind,target_id,purpose,main_question,subquestions_json,conditions_json,selected_note_ids_json,completion,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)", (target_kind, target_id, purpose, main, station.encode(subquestions), station.encode(conditions), station.encode(note_ids), completion, "needs_input" if needs_input else "ready", timestamp, timestamp))
            if needs_input:
                return redirect(f"/questions/{question_id}", "部署问题需要补充应用场景后才能检索")
            task_id = research.enqueue("question", job)
            db.execute("UPDATE research_questions SET task_id=?,status='running',updated_at=? WHERE id=?", (task_id, db.now(), question_id))
            return redirect(f"/questions/{question_id}")
        except (ValueError, TypeError) as error:
            return redirect(f"/questions/{question_id}" if question_id else "/questions", error=str(error))

    @router.get("/questions/{question_id}", response_class=HTMLResponse)
    def question_page(request: Request, question_id: RecordId):
        item = required("SELECT * FROM research_questions WHERE id=?", (question_id,))
        task = db.one("SELECT * FROM tasks WHERE id=?", (item["task_id"],)) if item["task_id"] else None
        documents = db.query("SELECT d.* FROM documents d JOIN document_task_links l ON l.document_id=d.id WHERE l.task_id=? ORDER BY d.id", (item["task_id"],)) if task else []
        conditions = {key: value for key, value in station.decode(item["conditions_json"]).items() if key != "research_context"}
        return page(request, "question.html", title=item["main_question"], item=item, task=task, result=station.decode(task["result_json"]) if task else {}, documents=documents, conditions=conditions, subquestions=station.decode(item["subquestions_json"]))

    @router.post("/questions/{question_id}/start")
    def start_question(question_id: RecordId, form=Depends(form_data)):
        item = required("SELECT * FROM research_questions WHERE id=?", (question_id,))
        try:
            if item["task_id"]:
                raise ValueError("该问题已经创建研究任务")
            scenario = text(form, "scenario", 200) if item["purpose"] == "部署决策" else str(form.get("scenario", "")).strip()[:200]
            conditions = station.decode(item["conditions_json"])
            if scenario:
                conditions["scenario"] = scenario
            job = {"target_kind": item["target_kind"], "target_id": item["target_id"], "purpose": item["purpose"], "main_question": item["main_question"], "subquestions": station.decode(item["subquestions_json"]), "conditions": conditions, "completion": item["completion"], "selected_note_ids": station.decode(item["selected_note_ids_json"])}
            task_id = research.enqueue("question", job)
            db.execute("UPDATE research_questions SET task_id=?,conditions_json=?,status='running',updated_at=? WHERE id=?", (task_id, station.encode(conditions), db.now(), question_id))
            return redirect(f"/questions/{question_id}")
        except ValueError as error:
            return redirect(f"/questions/{question_id}", error=str(error))

    @router.get("/personal", response_class=HTMLResponse)
    def personal(request: Request, q: str = "", archived: bool = False):
        notes = db.query("SELECT * FROM annotations WHERE archived=? AND body LIKE ? ORDER BY id DESC LIMIT 150", (int(archived), "%" + q[:100] + "%"))
        for note in notes:
            note["target"] = annotation_location(note)
        return page(request, "personal.html", title="个人区域", notes=notes, q=q, archived=archived, artifacts=db.query("SELECT a.*,t.title AS topic_title FROM artifacts a JOIN topics t ON t.id=a.topic_id ORDER BY a.id DESC LIMIT 15"), briefs=db.query("SELECT b.*,t.title AS topic_title FROM briefs b JOIN topics t ON t.id=b.topic_id ORDER BY b.id DESC LIMIT 15"))

    @router.post("/annotations")
    def add_annotation(form=Depends(form_data)):
        try:
            target_kind = text(form, "target_kind", 20)
            target_id = parse_record_id(form.get("target_id"))
            table = {"event": "events", "entity": "entities", "topic": "topics", "brief": "briefs", "artifact": "artifacts"}.get(target_kind)
            if not table:
                raise ValueError("批注目标无效")
            required(f"SELECT id FROM {table} WHERE id=?", (target_id,))
            body = text(form, "body", 20000)
            with db.transaction() as connection:
                cursor = connection.execute("INSERT INTO annotations(target_kind,target_id,body,created_at,updated_at) VALUES(?,?,?,?,?)", (target_kind, target_id, body, db.now(), db.now()))
                connection.execute("INSERT INTO annotation_versions(annotation_id,version,body,created_at) VALUES(?,1,?,?)", (cursor.lastrowid, body, db.now()))
            destination = annotation_location({"target_kind": target_kind, "target_id": target_id})["path"]
            return redirect(destination, "批注已保存到个人区域")
        except (ValueError, TypeError) as error:
            return redirect("/personal", error=str(error))

    @router.get("/annotations/{annotation_id}", response_class=HTMLResponse)
    def annotation_page(request: Request, annotation_id: RecordId):
        note = required("SELECT * FROM annotations WHERE id=?", (annotation_id,))
        versions = db.query("SELECT * FROM annotation_versions WHERE annotation_id=? ORDER BY version DESC", (annotation_id,))
        return page(request, "annotation.html", title="个人批注", note=note, target=annotation_location(note), versions=versions)

    @router.post("/annotations/{annotation_id}")
    def update_annotation(annotation_id: RecordId, form=Depends(form_data)):
        required("SELECT id FROM annotations WHERE id=?", (annotation_id,))
        try:
            body = text(form, "body", 20000)
            with db.transaction() as connection:
                next_version = connection.execute("SELECT coalesce(max(version),0)+1 FROM annotation_versions WHERE annotation_id=?", (annotation_id,)).fetchone()[0]
                connection.execute("INSERT INTO annotation_versions(annotation_id,version,body,created_at) VALUES(?,?,?,?)", (annotation_id, next_version, body, db.now()))
                connection.execute("UPDATE annotations SET body=?,updated_at=? WHERE id=?", (body, db.now(), annotation_id))
            return redirect(f"/annotations/{annotation_id}", "批注新版本已保存")
        except ValueError as error:
            return redirect(f"/annotations/{annotation_id}", error=str(error))

    @router.post("/annotations/{annotation_id}/archive")
    def archive_annotation(annotation_id: RecordId, form=Depends(form_data)):
        required("SELECT id FROM annotations WHERE id=?", (annotation_id,))
        archived = str(form.get("archive", "yes")) != "no"
        db.execute("UPDATE annotations SET archived=?,updated_at=? WHERE id=?", (int(archived), db.now(), annotation_id))
        return redirect("/personal", "批注已归档，历史版本仍保留" if archived else "批注已恢复到个人区域")

    app.include_router(router)
