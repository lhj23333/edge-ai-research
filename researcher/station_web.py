from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse

from . import db, station
from .config import get_profile, settings
from .providers import Source
from .scheduler import next_run


def rows_by_ids(table, ids):
    if not ids:
        return []
    return db.query(f"SELECT * FROM {table} WHERE id IN (" + ",".join("?" for _ in ids) + ") ORDER BY id DESC", tuple(ids))


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


def home_context():
    state = station.status()
    issue = db.one("SELECT * FROM daily_issues ORDER BY id DESC LIMIT 1")
    day_start = datetime.now(ZoneInfo(settings()["limits"]["timezone"])).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat(timespec="seconds")
    issues = db.query("SELECT * FROM daily_issues WHERE created_at>=? ORDER BY id", (day_start,))
    if not issues and issue:
        issues = [issue]
    event_ids = list(dict.fromkeys(value for item in issues for value in station.decode(item["event_ids_json"])))
    events = rows_by_ids("events", event_ids)
    if issue and issues:
        issue["coverage_start"] = min((item["coverage_start"] for item in issues if item["coverage_start"]), default=None)
        issue["coverage_end"] = max(item["coverage_end"] for item in issues)
    manual = db.query("SELECT DISTINCT e.* FROM events e JOIN event_claims ec ON ec.event_id=e.id JOIN claims c ON c.id=ec.claim_id JOIN documents d ON d.id=c.document_id WHERE e.state='active' AND d.extent='用户提供摘录' AND e.created_at>=? ORDER BY e.id DESC", (day_start,))
    existing_ids = {item["id"] for item in events}
    events.extend(item for item in manual if item["id"] not in existing_ids)
    events.sort(key=lambda item: (item["importance"], item["id"]), reverse=True)
    revisions = []
    if issue:
        ids = list(dict.fromkeys(value for item in issues for value in station.decode(item["revision_ids_json"])))
        if ids:
            revisions = db.query("SELECT v.*,t.title AS topic_title,j.topic_id,j.purpose FROM judgment_versions v JOIN judgments j ON j.id=v.judgment_id JOIN topics t ON t.id=j.topic_id WHERE j.current_version_id=v.id AND v.id IN (" + ",".join("?" for _ in ids) + ") ORDER BY v.id DESC", tuple(ids))
    extra = db.query("SELECT v.*,t.title AS topic_title,j.topic_id,j.purpose FROM judgments j JOIN judgment_versions v ON v.id=j.current_version_id JOIN topics t ON t.id=j.topic_id WHERE v.created_at>? ORDER BY v.id DESC LIMIT 6", (issue["created_at"] if issue else "1970-01-01",))
    revision_ids = {item["id"] for item in revisions}
    revisions.extend(item for item in extra if item["id"] not in revision_ids)
    revisions.sort(key=lambda item: item["id"], reverse=True)
    cutoff = state["last_visit_at"]
    catchup_events = db.query("SELECT id,title,created_at,background,occurred_at,published_at FROM events WHERE created_at>? AND state='active' ORDER BY id DESC LIMIT 20", (cutoff,)) if cutoff else []
    catchup_revisions = db.query("SELECT v.id,j.topic_id,t.title AS topic_title,v.reason,v.created_at FROM judgment_versions v JOIN judgments j ON j.id=v.judgment_id JOIN topics t ON t.id=j.topic_id WHERE v.created_at>? ORDER BY v.id DESC LIMIT 20", (cutoff,)) if cutoff else []
    progress = db.query("SELECT t.id,t.title,t.question,j.purpose,v.conclusion,v.status,v.created_at FROM topics t LEFT JOIN judgments j ON j.topic_id=t.id LEFT JOIN judgment_versions v ON v.id=j.current_version_id WHERE t.research_state='active' ORDER BY t.id,j.purpose")
    next_questions = station.decode(issue["next_questions_json"]) if issue else []
    latest_revision = db.one("SELECT result_json FROM tasks WHERE kind='revise' AND status='success' AND finished_at>? ORDER BY id DESC LIMIT 1", (issue["created_at"] if issue else day_start,))
    if latest_revision:
        next_questions = list(dict.fromkeys(station.decode(latest_revision["result_json"]).get("questions", []) + next_questions))
    return {"station": state, "issue": issue, "events": [item for item in events if not item["background"] and item["state"] == "active"][:5], "background_events": [item for item in events if item["background"] and item["state"] == "active"][:5], "revisions": revisions, "progress": progress, "next_questions": next_questions, "catchup_events": catchup_events, "catchup_revisions": catchup_revisions, "task_count": db.one("SELECT count(*) AS n FROM tasks WHERE status IN ('queued','running')")["n"]}


def register(app, page, form_data, redirect, required, text):
    router = APIRouter()

    @router.post("/reading/visit")
    def reading_visit(form=Depends(form_data)):
        db.execute("UPDATE station_settings SET last_visit_at=? WHERE id=1", (db.now(),))
        return JSONResponse({"status": "saved"})

    @router.post("/tasks/{task_id}/retry")
    def retry_task(task_id: int, form=Depends(form_data)):
        try:
            station.retry(task_id)
            return redirect(f"/tasks/{task_id}", "任务将继续使用原有调用额度")
        except ValueError as error:
            return redirect(f"/tasks/{task_id}", error=str(error))

    @router.get("/watchlist", response_class=HTMLResponse)
    def watchlist(request: Request):
        return page(request, "watchlist.html", title="关注与更新设置", station=station.status(), proposals=db.query("SELECT p.*,d.title AS document_title,d.url AS document_url FROM watchlist_proposals p LEFT JOIN documents d ON d.id=p.document_id ORDER BY p.kind,p.id"), entities=db.query("SELECT * FROM entities WHERE followed=1 ORDER BY kind,name"), topics=db.query("SELECT * FROM topics WHERE research_state='active' ORDER BY id"), coverage=station.decode(station.status()["coverage_json"]))

    @router.post("/watchlist/propose")
    def propose(form=Depends(form_data)):
        try:
            return redirect(f"/tasks/{station.enqueue('onboard')}")
        except ValueError as error:
            return redirect("/watchlist", error=str(error))

    @router.post("/watchlist/confirm")
    def confirm_watchlist(form=Depends(form_data)):
        try:
            selected = {int(value) for value in form.getlist("proposal_id")}
            if not selected and not db.one("SELECT id FROM entities WHERE followed=1 AND state='active' LIMIT 1") and not db.one("SELECT id FROM topics WHERE research_state='active' LIMIT 1"):
                raise ValueError("请至少选择一项厂商或专题")
            if str(form.get("confirm_sources")) != "yes" or str(form.get("confirm_model_budget")) != "yes":
                raise ValueError("请确认资料来源范围、当前模型和调用额度")
            clock = text(form, "schedule_time", 5)
            datetime.strptime(clock, "%H:%M")
            scope_text = text(form, "source_scope", 1000)
            if not get_profile().search_verified:
                raise ValueError("当前模型尚未完成联网测试")
            for proposal in db.query("SELECT * FROM watchlist_proposals WHERE status='accepted'"):
                if proposal["id"] in selected:
                    continue
                if proposal["kind"] == "company":
                    db.execute("UPDATE entities SET followed=0 WHERE kind='厂商' AND normalized_name=?", (station.normalized(proposal["name"]),))
                else:
                    db.execute("UPDATE topics SET research_state='archived',schedule='manual',next_run=NULL WHERE title=?", (proposal["name"],))
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
            db.execute("UPDATE station_settings SET watchlist_confirmed=1,sources_confirmed=1,auto_enabled=0,schedule_time=?,next_run=NULL,coverage_json=? WHERE id=1", (clock, station.encode({"source_scope": scope_text})))
            return redirect("/watchlist", "关注名单和采集条件已确认；自动更新仍未启用")
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
            return redirect("/watchlist", "关注对象已加入；请确认采集条件")
        except ValueError as error:
            return redirect("/watchlist", error=str(error))

    @router.post("/watchlist/automation")
    def automation(form=Depends(form_data)):
        try:
            state = station.status()
            enabled = str(form.get("enabled")) == "yes"
            if enabled and (not state["watchlist_confirmed"] or not state["sources_confirmed"] or not get_profile().search_verified):
                raise ValueError("请先确认关注名单、资料来源、模型和额度，并完成联网测试")
            clock = text(form, "schedule_time", 5)
            datetime.strptime(clock, "%H:%M")
            upcoming = next_run("daily", clock, 0, settings()["limits"]["timezone"]) if enabled else None
            db.execute("UPDATE station_settings SET auto_enabled=?,schedule_time=?,next_run=? WHERE id=1", (int(enabled), clock, upcoming))
            return redirect("/watchlist", "自动更新设置已保存")
        except ValueError as error:
            return redirect("/watchlist", error=str(error))

    @router.post("/updates/scan")
    def start_scan(form=Depends(form_data)):
        try:
            return redirect(f"/tasks/{station.enqueue('scan')}")
        except ValueError as error:
            return redirect("/updates", error=str(error))

    @router.get("/updates", response_class=HTMLResponse)
    def updates(request: Request, event_type: str = "", company: str = "", after: str = "", before: str = "", q: str = ""):
        filters = ["e.state='active'"]
        args = []
        if event_type:
            filters.append("e.event_type=?")
            args.append(event_type)
        if company:
            filters.append("EXISTS(SELECT 1 FROM event_entities ee JOIN entities x ON x.id=ee.entity_id WHERE ee.event_id=e.id AND x.name LIKE ?)")
            args.append("%" + company[:100] + "%")
        if after:
            filters.append("coalesce(e.occurred_at,e.published_at,e.created_at)>=?")
            args.append(after[:10])
        if before:
            filters.append("coalesce(e.occurred_at,e.published_at,e.created_at)<?")
            args.append(before[:10] + "T23:59:59")
        if q:
            filters.append("(e.title LIKE ? OR e.what_happened LIKE ?)")
            args.extend(["%" + q[:100] + "%"] * 2)
        events = db.query("SELECT e.*,(SELECT count(*) FROM event_claims WHERE event_id=e.id) AS evidence_count FROM events e WHERE " + " AND ".join(filters) + " ORDER BY e.importance DESC,e.id DESC LIMIT 150", tuple(args))
        return page(request, "updates.html", title="行业动态", events=events, station=station.status(), filters={"event_type": event_type, "company": company, "after": after, "before": before, "q": q}, event_types=sorted(station.EVENT_TYPES), runs=db.query("SELECT * FROM collection_runs ORDER BY id DESC LIMIT 6"))

    @router.post("/updates/manual")
    def manual_event(form=Depends(form_data)):
        try:
            title = text(form, "title", 180)
            what = text(form, "what_happened", 700)
            why = text(form, "why_important", 700)
            company = text(form, "company", 120)
            event_type = text(form, "event_type", 30)
            if event_type not in station.EVENT_TYPES:
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
            claim_id = station.save_claim(document_id, version_id, what, excerpt, "待核实")
            entity_id = station.entity("厂商", company)
            item = {"identity_key": f"{company}|{title}|{occurred}", "event_type": event_type, "title": title, "what_happened": what, "why_important": why, "competitive_impact": str(form.get("competitive_impact") or "")[:700], "occurred_at": occurred or None, "importance": 1, "background": bool(not occurred or occurred < station.coverage_window()[0][:10])}
            event_id, _ = station.add_event(item, [claim_id], [entity_id], [])
            return redirect(f"/events/{event_id}", "人工补充已保存，资料仍待核验")
        except (ValueError, TypeError) as error:
            return redirect("/updates", error=str(error))

    @router.get("/events/{event_id}", response_class=HTMLResponse)
    def event_page(request: Request, event_id: int):
        event = required("SELECT * FROM events WHERE id=?", (event_id,))
        if event["canonical_event_id"]:
            return redirect(f"/events/{event['canonical_event_id']}", "相同公告的来源已合并到此事件")
        claims = db.query("SELECT c.*,d.title AS document_title,d.url,d.published_at,d.last_fetched_at,d.extent FROM event_claims ec JOIN claims c ON c.id=ec.claim_id JOIN documents d ON d.id=c.document_id WHERE ec.event_id=? ORDER BY c.id", (event_id,))
        entities = db.query("SELECT x.* FROM event_entities ee JOIN entities x ON x.id=ee.entity_id WHERE ee.event_id=?", (event_id,))
        topics = db.query("SELECT t.*,et.impact FROM event_topics et JOIN topics t ON t.id=et.topic_id WHERE et.event_id=?", (event_id,))
        revisions = db.query("SELECT v.*,j.purpose,t.title AS topic_title,j.topic_id FROM judgment_versions v JOIN judgments j ON j.id=v.judgment_id JOIN topics t ON t.id=j.topic_id WHERE v.event_id=? OR v.event_id IN (SELECT id FROM events WHERE canonical_event_id=?) ORDER BY v.id DESC", (event_id, event_id))
        return page(request, "event.html", title=event["title"], event=event, claims=claims, entities=entities, topics=topics, revisions=revisions)

    @router.get("/evidence", response_class=HTMLResponse)
    def evidence(request: Request, q: str = "", label: str = ""):
        args = []
        where = []
        if q:
            where.append("(c.statement LIKE ? OR d.title LIKE ? OR d.url LIKE ?)")
            args.extend(["%" + q[:100] + "%"] * 3)
        if label:
            where.append("c.label=?")
            args.append(label)
        claims = db.query("SELECT c.*,d.title AS document_title,d.url,d.published_at,d.last_fetched_at,d.extent FROM claims c JOIN documents d ON d.id=c.document_id " + ("WHERE " + " AND ".join(where) if where else "") + " ORDER BY c.id DESC LIMIT 150", tuple(args))
        return page(request, "evidence.html", title="证据管理", claims=claims, documents=db.query("SELECT * FROM documents ORDER BY id DESC LIMIT 30"), q=q, label=label, labels=sorted(station.LABELS))

    @router.get("/documents/{document_id}", response_class=HTMLResponse)
    def document_page(request: Request, document_id: int):
        document = required("SELECT * FROM documents WHERE id=?", (document_id,))
        versions = db.query("SELECT id,extent,fetched_at,verification,content_hash FROM document_versions WHERE document_id=? ORDER BY id DESC", (document_id,))
        claims = db.query("SELECT c.*,d.title AS document_title,d.url,d.published_at,d.last_fetched_at,d.extent FROM claims c JOIN documents d ON d.id=c.document_id WHERE c.document_id=? ORDER BY c.id DESC", (document_id,))
        return page(request, "document.html", title=document["title"], document=document, versions=versions, claims=claims)

    @router.post("/claims/{claim_id}/verify")
    def verify_claim(claim_id: int, form=Depends(form_data)):
        claim = required("SELECT * FROM claims WHERE id=?", (claim_id,))
        choice = str(form.get("verification"))
        if choice not in {"人工核对原网页摘录", "人工核对发现不符", "待核实"}:
            return redirect(f"/documents/{claim['document_id']}", error="核验状态无效")
        verified_at = db.now() if choice != "待核实" else None
        db.execute("UPDATE claims SET verification=?,verified_at=? WHERE id=?", (choice, verified_at, claim_id))
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
        entities = db.query("SELECT e.*,(SELECT count(*) FROM entity_fields WHERE entity_id=e.id) AS field_count FROM entities e WHERE " + condition + " ORDER BY e.followed DESC,e.kind,e.name LIMIT 200", tuple(args))
        return page(request, "companies.html", title="厂商与产品", entities=entities, q=q, kind=kind)

    @router.get("/entities/{entity_id}", response_class=HTMLResponse)
    def entity_page(request: Request, entity_id: int):
        item = required("SELECT * FROM entities WHERE id=?", (entity_id,))
        fields = db.query("SELECT f.*,c.label,c.verification,d.url,d.title AS document_title FROM entity_fields f LEFT JOIN claims c ON c.id=f.claim_id LEFT JOIN documents d ON d.id=c.document_id WHERE f.entity_id=? ORDER BY f.field_name,f.id DESC", (entity_id,))
        relations = db.query("SELECT r.*,a.name AS source_name,b.name AS target_name,CASE WHEN r.from_entity_id=? THEN b.id ELSE a.id END AS other_id,c.label,d.url FROM relations r JOIN entities a ON a.id=r.from_entity_id JOIN entities b ON b.id=r.to_entity_id LEFT JOIN claims c ON c.id=r.claim_id LEFT JOIN documents d ON d.id=c.document_id WHERE r.from_entity_id=? OR r.to_entity_id=? ORDER BY r.id DESC", (entity_id, entity_id, entity_id))
        events = db.query("SELECT e.* FROM event_entities ee JOIN events e ON e.id=ee.event_id WHERE ee.entity_id=? AND e.state='active' ORDER BY e.id DESC LIMIT 20", (entity_id,))
        products = db.query("SELECT * FROM entities WHERE parent_id=? AND state!='archived' ORDER BY name", (entity_id,))
        return page(request, "entity.html", title=item["name"], item=item, fields=fields, relations=relations, events=events, products=products)

    @router.post("/entities/{entity_id}/follow")
    def follow_entity(entity_id: int, form=Depends(form_data)):
        item = required("SELECT * FROM entities WHERE id=?", (entity_id,))
        value = str(form.get("follow")) == "yes"
        db.execute("UPDATE entities SET followed=?,state=?,updated_at=? WHERE id=?", (int(value), "active" if value else item["state"], db.now(), entity_id))
        return redirect(f"/entities/{entity_id}", "关注状态已更新")

    @router.post("/entities/{entity_id}/archive")
    def archive_entity(entity_id: int, form=Depends(form_data)):
        required("SELECT id FROM entities WHERE id=?", (entity_id,))
        db.execute("UPDATE entities SET state='archived',followed=0,updated_at=? WHERE id=?", (db.now(), entity_id))
        return redirect("/companies", "资料已归档，历史记录仍可查阅")

    @router.get("/compare", response_class=HTMLResponse)
    def compare(request: Request):
        ids = [int(value) for value in request.query_params.getlist("id") if value.isdigit()]
        if len(ids) > 3:
            return page(request, "compare.html", title="产品对比", products=[], rows=[], comparable=False, error_message="一次最多选择三款产品")
        products = db.query("SELECT * FROM entities WHERE kind='产品' AND state!='archived' ORDER BY name")
        selected = rows_by_ids("entities", ids)
        fields = db.query("SELECT f.*,c.label,d.url FROM entity_fields f LEFT JOIN claims c ON c.id=f.claim_id LEFT JOIN documents d ON d.id=c.document_id WHERE f.entity_id IN (" + ",".join("?" for _ in ids) + ")", tuple(ids)) if ids else []
        by_product = {(item["entity_id"], item["field_name"]): item for item in fields}
        names = ["业务定位", "模型覆盖", "性能", "功耗成本", "软件迁移", "测试条件", "收费"]
        rows = [{"name": name, "values": [by_product.get((product["id"], name)) for product in selected]} for name in names]
        conditions = [station.normalized(by_product[(product["id"], "测试条件")]["value"]) for product in selected if (product["id"], "测试条件") in by_product]
        comparable = len(selected) > 1 and len(conditions) == len(selected) and len(set(conditions)) == 1
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
    def knowledge(request: Request, topic_id: int):
        topic = required("SELECT * FROM topics WHERE id=?", (topic_id,))
        judgments = db.query("SELECT j.*,v.conclusion,v.boundaries,v.unresolved,v.reason,v.created_at AS revised_at FROM judgments j LEFT JOIN judgment_versions v ON v.id=j.current_version_id WHERE j.topic_id=? ORDER BY j.purpose", (topic_id,))
        versions = db.query("SELECT v.*,j.purpose,(SELECT conclusion FROM judgment_versions prior WHERE prior.judgment_id=v.judgment_id AND prior.version=v.version-1) AS previous_conclusion FROM judgment_versions v JOIN judgments j ON j.id=v.judgment_id WHERE j.topic_id=? ORDER BY v.id DESC", (topic_id,))
        evidence = db.query("SELECT je.version_id,je.stance,j.current_version_id,j.purpose,c.*,d.title AS document_title,d.url,d.published_at,d.last_fetched_at,d.extent FROM judgment_evidence je JOIN judgment_versions v ON v.id=je.version_id JOIN judgments j ON j.id=v.judgment_id JOIN claims c ON c.id=je.claim_id JOIN documents d ON d.id=c.document_id WHERE j.topic_id=? ORDER BY je.version_id DESC", (topic_id,))
        events = db.query("SELECT e.*,et.impact FROM event_topics et JOIN events e ON e.id=et.event_id WHERE et.topic_id=? AND e.state='active' ORDER BY e.id DESC", (topic_id,))
        return page(request, "knowledge.html", title=topic["title"], topic=topic, judgments=judgments, versions=versions, evidence=evidence, current_evidence=[item for item in evidence if item["version_id"] == item["current_version_id"]], events=events)

    @router.post("/knowledge/{topic_id}/archive")
    def archive_topic(topic_id: int, form=Depends(form_data)):
        required("SELECT id FROM topics WHERE id=?", (topic_id,))
        db.execute("UPDATE topics SET research_state='archived',schedule='manual',next_run=NULL WHERE id=?", (topic_id,))
        return redirect("/map", "专题已归档，判断历史仍可查阅")

    @router.post("/knowledge/{topic_id}/revise")
    def revise_knowledge(topic_id: int, form=Depends(form_data)):
        required("SELECT id FROM topics WHERE id=?", (topic_id,))
        try:
            return redirect(f"/tasks/{station.enqueue('revise', {'topic_id': topic_id})}")
        except ValueError as error:
            return redirect(f"/knowledge/{topic_id}", error=str(error))

    @router.get("/questions", response_class=HTMLResponse)
    def questions(request: Request, target_kind: str = "topic", target_id: int = 0, action: str = "", event_id: int = 0, claim_id: int = 0):
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
        return page(request, "questions.html", title="定向研究", target_kind=target_kind, target_id=target_id, event_id=event["id"] if event else 0, claim_id=claim_id if claim else 0, action=action, prefill=prefill, topics=db.query("SELECT id,title FROM topics WHERE research_state='active' OR id=? ORDER BY title", (target_id if target_kind == "topic" else 0,)), entities=db.query("SELECT id,name,kind FROM entities WHERE state!='archived' OR id=? ORDER BY kind,name", (target_id if target_kind == "entity" else 0,)), notes=notes, questions=db.query("SELECT * FROM research_questions ORDER BY id DESC LIMIT 30"))

    @router.post("/questions")
    def create_question(form=Depends(form_data)):
        question_id = None
        try:
            target_kind = text(form, "target_kind", 20)
            target_id = int(form.get("target_id", 0))
            if target_kind not in {"topic", "entity"} or not target_id:
                raise ValueError("请选择具体的专题、厂商或产品")
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
                value = int(form.get(key, 0) or 0)
                if value:
                    required(f"SELECT id FROM {table} WHERE id=?", (value,))
                    research_context[key] = value
            if research_context:
                conditions["research_context"] = research_context
            note_ids = [int(value) for value in form.getlist("note_id") if value.isdigit()]
            for note_id in note_ids:
                if not db.one("SELECT id FROM annotations WHERE id=? AND archived=0", (note_id,)):
                    raise ValueError("选中的个人笔记不存在")
            job = {"target_kind": target_kind, "target_id": target_id, "purpose": purpose, "main_question": main, "subquestions": subquestions, "conditions": conditions, "completion": completion, "selected_note_ids": note_ids}
            needs_input = purpose == "部署决策" and not conditions.get("scenario")
            timestamp = db.now()
            question_id = db.execute("INSERT INTO research_questions(target_kind,target_id,purpose,main_question,subquestions_json,conditions_json,selected_note_ids_json,completion,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)", (target_kind, target_id, purpose, main, station.encode(subquestions), station.encode(conditions), station.encode(note_ids), completion, "needs_input" if needs_input else "ready", timestamp, timestamp))
            if needs_input:
                return redirect(f"/questions/{question_id}", "部署问题需要补充应用场景后才能检索")
            task_id = station.enqueue("question", job)
            db.execute("UPDATE research_questions SET task_id=?,status='running',updated_at=? WHERE id=?", (task_id, db.now(), question_id))
            return redirect(f"/questions/{question_id}")
        except (ValueError, TypeError) as error:
            return redirect(f"/questions/{question_id}" if question_id else "/questions", error=str(error))

    @router.get("/questions/{question_id}", response_class=HTMLResponse)
    def question_page(request: Request, question_id: int):
        item = required("SELECT * FROM research_questions WHERE id=?", (question_id,))
        task = db.one("SELECT * FROM tasks WHERE id=?", (item["task_id"],)) if item["task_id"] else None
        documents = db.query("SELECT d.* FROM documents d JOIN document_task_links l ON l.document_id=d.id WHERE l.task_id=? ORDER BY d.id", (item["task_id"],)) if task else []
        conditions = {key: value for key, value in station.decode(item["conditions_json"]).items() if key != "research_context"}
        return page(request, "question.html", title=item["main_question"], item=item, task=task, result=station.decode(task["result_json"]) if task else {}, documents=documents, conditions=conditions, subquestions=station.decode(item["subquestions_json"]))

    @router.post("/questions/{question_id}/start")
    def start_question(question_id: int, form=Depends(form_data)):
        item = required("SELECT * FROM research_questions WHERE id=?", (question_id,))
        try:
            if item["task_id"]:
                raise ValueError("该问题已经创建研究任务")
            scenario = text(form, "scenario", 200) if item["purpose"] == "部署决策" else str(form.get("scenario", "")).strip()[:200]
            conditions = station.decode(item["conditions_json"])
            if scenario:
                conditions["scenario"] = scenario
            job = {"target_kind": item["target_kind"], "target_id": item["target_id"], "purpose": item["purpose"], "main_question": item["main_question"], "subquestions": station.decode(item["subquestions_json"]), "conditions": conditions, "completion": item["completion"], "selected_note_ids": station.decode(item["selected_note_ids_json"])}
            task_id = station.enqueue("question", job)
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
            target_id = int(form.get("target_id", 0))
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
    def annotation_page(request: Request, annotation_id: int):
        note = required("SELECT * FROM annotations WHERE id=?", (annotation_id,))
        versions = db.query("SELECT * FROM annotation_versions WHERE annotation_id=? ORDER BY version DESC", (annotation_id,))
        return page(request, "annotation.html", title="个人批注", note=note, target=annotation_location(note), versions=versions)

    @router.post("/annotations/{annotation_id}")
    def update_annotation(annotation_id: int, form=Depends(form_data)):
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
    def archive_annotation(annotation_id: int, form=Depends(form_data)):
        required("SELECT id FROM annotations WHERE id=?", (annotation_id,))
        archived = str(form.get("archive", "yes")) != "no"
        db.execute("UPDATE annotations SET archived=?,updated_at=? WHERE id=?", (int(archived), db.now(), annotation_id))
        return redirect("/personal", "批注已归档，历史版本仍保留" if archived else "批注已恢复到个人区域")

    app.include_router(router)
