import asyncio
import hashlib
import hmac
import json
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import bleach
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from markdown_it import MarkdownIt
from markupsafe import Markup
from pydantic import ValidationError
from sse_starlette.sse import EventSourceResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware

from . import __version__, db, research
from .config import MODEL_STEPS, PRESETS, Limits, Profile, config_lock, describe_routes, get_profile, model_routes, save_profile, settings, write_json
from .service import KINDS, STATES, enqueue_test, save_artifact, task_documents, usage_summary
from .scheduler import next_topic_run, pause_topic_schedule
from .web_ids import RecordId, parse_record_id

HERE = Path(__file__).parent
STARTED_AT = datetime.now(timezone.utc).isoformat(timespec="seconds")
templates = Jinja2Templates(directory=str(HERE / "templates"))
markdown = MarkdownIt("commonmark", {"html": False}).enable("table")


def render_markdown(value):
    html = markdown.render(value or "")
    clean = bleach.clean(html, tags={"p", "br", "strong", "em", "ul", "ol", "li", "blockquote", "pre", "code", "h1", "h2", "h3", "h4", "a", "table", "thead", "tbody", "tr", "th", "td", "hr", "s"}, attributes={"a": ["href", "title"], "th": ["align"], "td": ["align"]}, protocols=["http", "https"], strip=True)
    return Markup(clean)


def local_time(value):
    from zoneinfo import ZoneInfo
    if not value:
        return "尚未执行"
    return datetime.fromisoformat(value).astimezone(ZoneInfo(settings()["limits"]["timezone"])).strftime("%Y-%m-%d %H:%M")


def asset_version():
    digest = hashlib.sha256()
    root = HERE / "static"
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()[:12]


def build_id():
    digest = hashlib.sha256()
    for path in sorted(HERE.rglob("*")):
        if path.suffix in {".py", ".sql", ".html"} and path.is_file():
            digest.update(path.relative_to(HERE).as_posix().encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()[:12]


BUILD_ID = build_id()


TONES = {
    "queued": "neutral", "running": "running", "success": "success", "failed": "danger", "cancelled": "neutral", "interrupted": "warning",
    "complete": "success", "partial": "warning", "never": "neutral", "needs_input": "warning", "ready": "neutral", "answered": "success",
    "公开披露": "success", "厂商宣称": "warning", "分析判断": "accent", "待核实": "pending",
    "人工核对原网页摘录": "success", "人工核对发现不符": "danger",
    "初步判断": "accent", "得到加强": "success", "需要修正": "warning", "存在争议": "danger",
    "支持": "success", "反对": "danger",
}
KIND_KEYS = {"厂商": "company", "产品": "product", "应用场景": "scenario", "技术路线": "route", "软件生态": "software"}
NAVIGATION = (
    {"label": "Workspace", "links": (
        {"href": "/", "label": "今日简报", "icon": "overview", "match": ("/",)},
        {"href": "/updates", "label": "行业动态", "icon": "pulse", "match": ("/updates", "/events")},
        {"href": "/map", "label": "认知地图", "icon": "graph", "match": ("/map", "/knowledge")},
    )},
    {"label": "Hardware", "links": (
        {"href": "/companies", "label": "厂商与产品", "icon": "chip", "match": ("/companies", "/entities"), "children": (
            {"href": "/compare", "label": "产品对比", "match": ("/compare",)},
        )},
        {"href": "/evidence", "label": "证据管理", "icon": "evidence", "match": ("/evidence", "/documents")},
    )},
    {"label": "Research", "links": (
        {"href": "/research", "label": "探索研究", "icon": "search", "match": ("/research",)},
        {"href": "/questions", "label": "定向研究", "icon": "target", "match": ("/questions",)},
        {"href": "/personal", "label": "个人区域", "icon": "note", "match": ("/personal", "/annotations"), "children": (
            {"href": "/library", "label": "成果库", "match": ("/library", "/artifacts")},
            {"href": "/topics", "label": "历史主题", "match": ("/topics", "/briefs")},
        )},
    )},
    {"label": "System", "system": True, "links": (
        {"href": "/watchlist", "label": "关注与更新设置", "icon": "eye", "match": ("/watchlist",)},
        {"href": "/admin/sources", "label": "信源管理", "icon": "link", "match": ("/admin/sources",), "children": (
            {"href": "/admin/funnel", "label": "漏斗诊断", "match": ("/admin/funnel",)},
            {"href": "/admin/eval", "label": "SelectBench 评测", "match": ("/admin/eval",)},
        )},
        {"href": "/tasks", "label": "任务记录", "icon": "terminal", "match": ("/tasks",)},
        {"href": "/settings", "label": "模型与运行设置", "icon": "sliders", "match": ("/settings",)},
    )},
)


def navigation(path):
    def hit(link):
        return any(path == prefix if prefix == "/" else path.startswith(prefix) for prefix in link["match"])

    groups, trail = [], []
    for group in NAVIGATION:
        links = []
        for link in group["links"]:
            children = [dict(child, current=hit(child)) for child in link.get("children", ())]
            current = hit(link)
            active_child = next((child for child in children if child["current"]), None)
            if current or active_child:
                trail = [(group["label"], None), (link["label"], link["href"])] + ([(active_child["label"], active_child["href"])] if active_child else [])
            links.append(dict(link, current=current, expanded=current or bool(active_child), children=children))
        groups.append(dict(group, links=links))
    return {"groups": groups, "trail": trail}


templates.env.filters["markdown"] = render_markdown
templates.env.filters["localtime"] = local_time
templates.env.filters["tone"] = lambda value: TONES.get(value, "neutral")
templates.env.filters["kind_key"] = lambda value: KIND_KEYS.get(value, "other")
templates.env.globals.update(states=STATES, kinds=KINDS, presets=PRESETS, asset_version=asset_version())


@asynccontextmanager
async def lifespan(app):
    db.initialize()
    yield


app = FastAPI(title="Researcher Web", version=__version__, lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=["127.0.0.1", "localhost", "[::1]"])
app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")


@app.middleware("http")
async def security(request, call_next):
    token = request.cookies.get("researcher_csrf") or secrets.token_urlsafe(32)
    request.state.csrf = token
    if request.method not in ("GET", "HEAD", "OPTIONS"):
        origin = request.headers.get("origin")
        if request.headers.get("sec-fetch-site") == "cross-site" or (origin and origin != f"{request.url.scheme}://{request.headers.get('host')}"):
            return PlainTextResponse("拒绝来自其他站点的操作请求", status_code=403)
    response = await call_next(request)
    if not request.cookies.get("researcher_csrf"):
        response.set_cookie("researcher_csrf", token, httponly=True, samesite="strict")
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
    response.headers["Cache-Control"] = "no-store" if not request.url.path.startswith("/static/") else "public, max-age=3600"
    return response


async def form_data(request: Request):
    if int(request.headers.get("content-length", "0")) > 300000:
        raise HTTPException(413, "提交内容超过大小限制")
    form = await request.form(max_fields=100, max_files=0)
    provided = str(form.get("csrf", ""))
    if not hmac.compare_digest(request.cookies.get("researcher_csrf", "").encode(), provided.encode()) or not provided:
        raise HTTPException(403, "操作验证已失效，请刷新页面后重试")
    return form


def worker_alive():
    heartbeat = db.one("SELECT value FROM runtime WHERE name='worker_heartbeat'")
    return bool(heartbeat and (datetime.now(timezone.utc) - datetime.fromisoformat(heartbeat["value"])).total_seconds() < 20)


def page(request, name, **values):
    return templates.TemplateResponse(request=request, name=name, context={
        "config": settings(), "worker_alive": worker_alive(),
        "csrf": request.state.csrf, "notice": request.query_params.get("notice"),
        "error": request.query_params.get("error"), "path": request.url.path, "nav": navigation(request.url.path), **values,
    })


def redirect(path, notice=None, error=None):
    if notice or error:
        path += ("&" if "?" in path else "?") + urlencode({"error" if error else "notice": error or notice})
    return RedirectResponse(path, status_code=303)


def required(sql, parameters):
    item = db.one(sql, parameters)
    if not item:
        raise HTTPException(404, "内容不存在")
    return item


def text(form, name, maximum, default=""):
    value = str(form.get(name, default)).strip()
    if not value or len(value) > maximum:
        raise ValueError(f"{name} 不能为空，且不能超过 {maximum} 个字符")
    return value


@app.exception_handler(HTTPException)
async def http_error(request, error):
    return templates.TemplateResponse(request=request, name="error.html", context={"message": str(error.detail), "status": error.status_code}, status_code=error.status_code)


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    from .station_web import home_context
    return page(request, "home.html", title="今日简报", **home_context())


@app.get("/topics", response_class=HTMLResponse)
def topic_list(request: Request):
    topics = db.query("SELECT t.*,(SELECT count(*) FROM briefs WHERE topic_id=t.id) AS brief_count FROM topics t ORDER BY t.id DESC")
    return page(request, "topics.html", title="关注主题", topics=topics)


@app.post("/topics")
def create_topic(form=Depends(form_data)):
    try:
        title = text(form, "title", 160)
        question = text(form, "question", 3000)
        kind = str(form.get("kind", "专题"))
        if kind not in ("领域", "厂商", "产品", "专题"):
            raise ValueError("请选择有效的主题类别")
        topic_id = db.execute("INSERT INTO topics(title,question,kind,created_at) VALUES(?,?,?,?)", (title, question, kind, db.now()))
        return redirect(f"/topics/{topic_id}", "主题已保存，可以开始研究")
    except ValueError as error:
        return redirect("/topics", error=str(error))


@app.get("/topics/{topic_id}", response_class=HTMLResponse)
def topic_detail(request: Request, topic_id: RecordId):
    topic = required("SELECT * FROM topics WHERE id=?", (topic_id,))
    return page(request, "topic.html", title=topic["title"], topic=topic,
        plan=db.one("SELECT * FROM topic_research_schedules WHERE topic_id=?", (topic_id,)),
        automation=db.one("SELECT auto_enabled,watchlist_confirmed,sources_confirmed FROM station_settings WHERE id=1"),
        briefs=db.query("SELECT * FROM briefs WHERE topic_id=? ORDER BY id DESC", (topic_id,)),
        messages=db.query("SELECT m.*,t.status AS task_status,t.error AS task_error FROM messages m LEFT JOIN tasks t ON t.id=m.task_id WHERE m.topic_id=? ORDER BY m.id", (topic_id,)),
        artifacts=db.query("SELECT * FROM artifacts WHERE topic_id=? ORDER BY id DESC", (topic_id,)),
        tasks=db.query("SELECT * FROM tasks WHERE topic_id=? ORDER BY id DESC LIMIT 6", (topic_id,)))


@app.post("/topics/{topic_id}/tasks")
def topic_task(topic_id: RecordId, form=Depends(form_data)):
    topic = required("SELECT * FROM topics WHERE id=?", (topic_id,))
    try:
        kind = form.get("kind", "research")
        if kind not in {"research", "chat", "summary"}:
            raise ValueError("任务类型无效")
        brief_id = parse_record_id(form["brief_id"]) if form.get("brief_id") else None
        if brief_id and not db.one("SELECT id FROM briefs WHERE id=? AND topic_id=?", (brief_id, topic_id)):
            raise ValueError("简报与当前专题不匹配")
        job = research.topic_job(topic)
        if kind == "chat" or form.get("prompt"):
            job["main_question"] = text(form, "prompt", 10000)
        elif kind == "research":
            job["main_question"] = f"定向研究《{topic['title']}》：{topic['question']}。先查阅已有原文，仅针对证据缺口补充核验。"
        task_id = research.enqueue("revise" if kind == "summary" else "interactive", job, brief_id=brief_id)
        return redirect(f"/tasks/{task_id}")
    except ValueError as error:
        return redirect(f"/topics/{topic_id}", error=str(error))


@app.post("/topics/{topic_id}/schedule")
def topic_schedule(topic_id: RecordId, form=Depends(form_data)):
    destination = f"/topics/{topic_id}"
    required("SELECT id FROM topics WHERE id=?", (topic_id,))
    frequency = form.get("schedule")
    action = form.get("action", "pause" if frequency == "manual" else "enable")
    try:
        if action == "pause":
            with db.transaction() as connection:
                pause_topic_schedule(connection, topic_id)
            return redirect(destination, "专题定向更新计划已停用")
        clock = str(form.get("schedule_time", "09:00"))
        weekday = int(str(form.get("weekday", "0")))
        if action not in {"save", "enable"} or frequency not in {"daily", "weekly"} or weekday not in range(7):
            raise ValueError("请选择有效的更新频率和星期")
        if len(clock) != 5 or datetime.strptime(clock, "%H:%M").strftime("%H:%M") != clock:
            raise ValueError("请按 HH:MM 格式填写更新时间")
        upcoming = next_topic_run(frequency, clock, weekday, settings()["limits"]["timezone"])
        with db.transaction() as connection:
            if connection.execute("SELECT research_state FROM topics WHERE id=?", (topic_id,)).fetchone()["research_state"] == "archived":
                raise ValueError("已归档专题无法设置定向更新计划")
            current = connection.execute("SELECT next_run FROM topic_research_schedules WHERE topic_id=?", (topic_id,)).fetchone()
            enabled = action == "enable" or bool(current and current["next_run"])
            connection.execute("""
                INSERT INTO topic_research_schedules(topic_id,frequency,schedule_time,weekday,next_run) VALUES(?,?,?,?,?)
                ON CONFLICT(topic_id) DO UPDATE SET frequency=excluded.frequency,schedule_time=excluded.schedule_time,
                                                   weekday=excluded.weekday,next_run=excluded.next_run
            """, (topic_id, frequency, clock, weekday, upcoming if enabled else None))
        return redirect(destination, "专题定向更新计划已启用" if action == "enable" else "专题定向更新计划已保存")
    except ValueError as error:
        return redirect(destination, error=str(error))


@app.post("/topics/{topic_id}/edit")
def edit_topic(topic_id: RecordId, form=Depends(form_data)):
    required("SELECT * FROM topics WHERE id=?", (topic_id,))
    try:
        db.execute("UPDATE topics SET title=?,question=? WHERE id=?", (text(form, "title", 160), text(form, "question", 3000), topic_id))
        return redirect(f"/topics/{topic_id}", "关注方向已更新")
    except ValueError as error:
        return redirect(f"/topics/{topic_id}", error=str(error))


@app.post("/topics/{topic_id}/observations")
def observation(topic_id: RecordId, form=Depends(form_data)):
    required("SELECT * FROM topics WHERE id=?", (topic_id,))
    try:
        body = text(form, "body", 20000)
        with db.transaction() as connection:
            cursor = connection.execute("INSERT INTO annotations(target_kind,target_id,body,created_at,updated_at) VALUES('topic',?,?,?,?)", (topic_id, body, db.now(), db.now()))
            connection.execute("INSERT INTO annotation_versions(annotation_id,version,body,created_at) VALUES(?,?,?,?)", (cursor.lastrowid, 1, body, db.now()))
        return redirect(f"/topics/{topic_id}", "个人观察已保存到个人区域")
    except ValueError as error:
        return redirect(f"/topics/{topic_id}", error=str(error))


@app.get("/briefs/{brief_id}", response_class=HTMLResponse)
def brief_page(request: Request, brief_id: RecordId):
    brief = required("SELECT b.*,t.title AS topic_title FROM briefs b JOIN topics t ON t.id=b.topic_id WHERE b.id=?", (brief_id,))
    return page(request, "brief.html", title=brief["title"], brief=brief, sources=task_documents(brief["task_id"]))


@app.post("/briefs/{brief_id}/save")
def save_brief(brief_id: RecordId, form=Depends(form_data)):
    brief = required("SELECT * FROM briefs WHERE id=?", (brief_id,))
    sources = task_documents(brief["task_id"])
    appendix = "\n\n## 来源记录\n" + "\n".join(f"- [{source['title']}]({source['url']})；发布日期：{source['published_at'] or '未提供'}；获取时间：{source['fetched_at']}；{source['extent']}" for source in sources)
    artifact_id = save_artifact(brief["topic_id"], brief["title"], brief["body"] + appendix, brief["task_id"])
    return redirect(f"/artifacts/{artifact_id}", "已保存为新的成果版本")


@app.get("/library", response_class=HTMLResponse)
def library(request: Request, q: str = ""):
    q = q.strip()[:200]
    results = db.query("SELECT a.*,t.title AS topic_title FROM artifacts a JOIN topics t ON t.id=a.topic_id WHERE a.title LIKE ? OR a.body LIKE ? ORDER BY a.id DESC LIMIT 100", (f"%{q}%", f"%{q}%"))
    return page(request, "library.html", title="成果库", artifacts=results, q=q)


@app.get("/artifacts/{artifact_id}", response_class=HTMLResponse)
def artifact_page(request: Request, artifact_id: RecordId):
    artifact = required("SELECT a.*,t.title AS topic_title FROM artifacts a JOIN topics t ON t.id=a.topic_id WHERE a.id=?", (artifact_id,))
    return page(request, "artifact.html", title=artifact["title"], artifact=artifact, versions=db.query("SELECT id,title,version,created_at FROM artifacts WHERE topic_id=? ORDER BY id DESC", (artifact["topic_id"],)))


@app.post("/artifacts/{artifact_id}")
def edit_artifact(artifact_id: RecordId, form=Depends(form_data)):
    artifact = required("SELECT * FROM artifacts WHERE id=?", (artifact_id,))
    try:
        new_id = save_artifact(artifact["topic_id"], text(form, "title", 180), text(form, "body", 100000), artifact["origin_task"])
        return redirect(f"/artifacts/{new_id}", "已保存新版本，之前的版本仍可查阅")
    except ValueError as error:
        return redirect(f"/artifacts/{artifact_id}", error=str(error))


@app.get("/artifacts/{artifact_id}/export")
def export_artifact(artifact_id: RecordId):
    artifact = required("SELECT * FROM artifacts WHERE id=?", (artifact_id,))
    body = f"# {artifact['title']}\n\n版本：{artifact['version']}\n保存时间：{artifact['created_at']}\n\n{artifact['body']}\n"
    return PlainTextResponse(body, headers={"Content-Disposition": f'attachment; filename="research-{artifact_id}-v{artifact["version"]}.md"'}, media_type="text/markdown")


@app.get("/tasks", response_class=HTMLResponse)
def task_list(request: Request):
    tasks = db.query("SELECT x.*,t.title AS topic_title FROM tasks x LEFT JOIN topics t ON t.id=x.topic_id ORDER BY x.id DESC LIMIT 100")
    return page(request, "tasks.html", title="研究任务", tasks=tasks, usage=usage_summary(), scheduler_error=db.one("SELECT value FROM runtime WHERE name='scheduler_error'"))


@app.get("/tasks/{task_id}", response_class=HTMLResponse)
def task_page(request: Request, task_id: RecordId):
    task = required("SELECT x.*,t.title AS topic_title FROM tasks x LEFT JOIN topics t ON t.id=x.topic_id WHERE x.id=?", (task_id,))
    return page(request, "task.html", title=KINDS.get(task["kind"], "研究任务"), task=task,
        brief=db.one("SELECT id FROM briefs WHERE task_id=?", (task_id,)),
        artifact=db.one("SELECT id FROM artifacts WHERE origin_task=? ORDER BY id DESC LIMIT 1", (task_id,)),
        calls=db.query("SELECT * FROM usage WHERE task_id=? ORDER BY id", (task_id,)),
        steps=db.query("SELECT * FROM task_steps WHERE task_id=? ORDER BY id", (task_id,)),
        coverage=db.query("SELECT c.* FROM coverage_checks c JOIN collection_runs r ON r.id=c.run_id WHERE r.task_id=? ORDER BY c.id", (task_id,)),
        feed_runs=db.query("SELECT r.*,s.name FROM feed_runs r JOIN feed_sources s ON s.id=r.source_id WHERE r.task_id=? ORDER BY r.id", (task_id,)),
        documents=task_documents(task_id))


@app.get("/tasks/{task_id}/events")
async def task_events(request: Request, task_id: RecordId):
    required("SELECT id FROM tasks WHERE id=?", (task_id,))

    async def stream():
        last = None
        for _ in range(1200):
            if await request.is_disconnected():
                return
            task = db.one("SELECT status,progress,result_text,error FROM tasks WHERE id=?", (task_id,))
            payload = json.dumps(task, ensure_ascii=False)
            if payload != last:
                yield {"event": "task", "data": payload}
                last = payload
            if task["status"] not in ("queued", "running"):
                return
            await asyncio.sleep(1)

    return EventSourceResponse(stream(), ping=15)


@app.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: RecordId, form=Depends(form_data)):
    task = required("SELECT * FROM tasks WHERE id=?", (task_id,))
    with db.transaction() as connection:
        connection.execute("UPDATE tasks SET cancel_requested=1 WHERE id=? AND status IN ('queued','running')", (task_id,))
        connection.execute("UPDATE tasks SET status='cancelled',progress='已取消',finished_at=? WHERE id=? AND status='queued'", (db.now(), task_id))
        if task["status"] == "queued" and task["session_id"]:
            connection.execute("UPDATE research_sessions SET status='cancelled',updated_at=? WHERE id=?", (db.now(), task["session_id"]))
    if task["status"] == "queued" and task["session_id"]:
        from .research import finish_gap
        finish_gap(task, False)
    return redirect(f"/tasks/{task_id}", "已提交取消请求")


@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    return page(request, "settings.html", title="模型与运行设置", usage=usage_summary(),
                routes=list(describe_routes(settings())), route_usage={row["route"]: row for row in usage_summary(by_route=True)})


@app.post("/settings/profiles")
def profile_save(form=Depends(form_data)):
    try:
        profile_id = str(form.get("id") or secrets.token_hex(8))
        profile = Profile(id=profile_id, name=text(form, "name", 80), provider=text(form, "provider", 30), base_url=text(form, "base_url", 500), model=text(form, "model", 120), reasoning_effort=str(form.get("reasoning_effort", "low")), input_price=float(form["input_price"]) if form.get("input_price") else None, output_price=float(form["output_price"]) if form.get("output_price") else None, price_note=str(form.get("price_note", "")), search_price=float(form["search_price"]) if form.get("search_price") else None)
        key = str(form.get("api_key", "")).strip()
        if len(key) > 2000 or "\n" in key or "\r" in key:
            raise ValueError("API 密钥格式无效")
        save_profile(profile, key)
        return redirect("/settings", "模型配置已保存；修改模型、地址或密钥后需要重新测试")
    except ValidationError:
        return redirect("/settings", error="配置格式无效，请检查名称、HTTPS 地址、模型和价格设置")
    except ValueError as error:
        return redirect("/settings", error=str(error))


@app.post("/settings/active")
def active_profile(form=Depends(form_data)):
    try:
        profile = get_profile(str(form.get("profile_id")))
        with config_lock():
            config = settings()
            config["active_profile"] = profile.id
            write_json("config.json", config)
        return redirect("/settings", "当前使用的模型已更新")
    except ValueError as error:
        return redirect("/settings", error=str(error))


@app.post("/settings/test")
def test_profile(form=Depends(form_data)):
    try:
        mode = str(form.get("mode"))
        if mode not in ("test_chat", "test_search", "test_tools"):
            raise ValueError("测试类型无效")
        task_id = enqueue_test(mode, str(form.get("profile_id")))
        return redirect(f"/tasks/{task_id}")
    except ValueError as error:
        return redirect("/settings", error=str(error))


@app.post("/settings/limits")
def limits_save(form=Depends(form_data)):
    try:
        with config_lock():
            config = settings()
            values = {key: form.get(key, config["limits"][key]) for key in ("daily_requests", "task_requests", "output_tokens", "timezone")}
            values.update({key: form.get(key) or None if key in form else config["limits"][key] for key in ("minute_cost", "hour_cost", "daily_cost")})
            config["limits"] = Limits(**values).model_dump()
            write_json("config.json", config)
        return redirect("/settings", "调用额度已保存，后续任务使用新设置")
    except ValidationError as error:
        if any(issue["loc"] == ("timezone",) for issue in error.errors(include_input=False)):
            return redirect("/settings", error="时区无效，请填写有效的 IANA 时区名称")
        return redirect("/settings", error="额度格式无效，请填写页面允许范围内的整数")


@app.post("/settings/routes")
def save_routes(form=Depends(form_data)):
    try:
        with config_lock():
            config = settings()
            entries = {}
            for step in MODEL_STEPS:
                fields = {key: form.get(f"{step}_{key}") or None for key in ("profile_id", "reasoning_effort", "output_tokens")}
                if fields["profile_id"]:
                    get_profile(fields["profile_id"], config)
                entries[step] = fields
            config["model_routes"] = model_routes(entries)
            write_json("config.json", config)
        return redirect("/settings#routes", "模型步骤路由已保存")
    except ValidationError:
        return redirect("/settings#routes", error="请检查步骤的模型、思考强度与输出上限")
    except ValueError as error:
        return redirect("/settings#routes", error=str(error))


@app.get("/health")
def health():
    return {
        "status": "ok", "version": __version__, "build_id": BUILD_ID, "started_at": STARTED_AT,
        "schema_version": db.one("PRAGMA user_version")["user_version"],
        "worker": "running" if worker_alive() else "stopped",
    }


from .station_web import register

register(app, page, form_data, redirect, required, text)

from .research_web import register as register_research

register_research(app, page, form_data, redirect, required, text)

from .ingest.web import register as register_ingest

register_ingest(app, page, form_data, redirect, required, text)
