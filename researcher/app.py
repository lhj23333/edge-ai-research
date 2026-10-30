import asyncio
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

from . import __version__, db
from .config import PRESETS, Limits, Profile, config_lock, get_profile, save_profile, settings, write_json
from .scheduler import next_run
from .service import KINDS, STATES, enqueue, save_artifact, usage_summary

HERE = Path(__file__).parent
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


templates.env.filters["markdown"] = render_markdown
templates.env.filters["localtime"] = local_time
templates.env.globals.update(states=STATES, kinds=KINDS, presets=PRESETS)


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
        "error": request.query_params.get("error"), "path": request.url.path, **values,
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
def topic_detail(request: Request, topic_id: int):
    topic = required("SELECT * FROM topics WHERE id=?", (topic_id,))
    return page(request, "topic.html", title=topic["title"], topic=topic,
        briefs=db.query("SELECT * FROM briefs WHERE topic_id=? ORDER BY id DESC", (topic_id,)),
        messages=db.query("SELECT m.*,t.status AS task_status,t.error AS task_error FROM messages m LEFT JOIN tasks t ON t.id=m.task_id WHERE m.topic_id=? ORDER BY m.id", (topic_id,)),
        artifacts=db.query("SELECT * FROM artifacts WHERE topic_id=? ORDER BY id DESC", (topic_id,)),
        tasks=db.query("SELECT * FROM tasks WHERE topic_id=? ORDER BY id DESC LIMIT 6", (topic_id,)))


@app.post("/topics/{topic_id}/edit")
def edit_topic(topic_id: int, form=Depends(form_data)):
    required("SELECT * FROM topics WHERE id=?", (topic_id,))
    try:
        db.execute("UPDATE topics SET title=?,question=? WHERE id=?", (text(form, "title", 160), text(form, "question", 3000), topic_id))
        return redirect(f"/topics/{topic_id}", "关注方向已更新")
    except ValueError as error:
        return redirect(f"/topics/{topic_id}", error=str(error))


@app.post("/topics/{topic_id}/schedule")
def schedule(topic_id: int, form=Depends(form_data)):
    required("SELECT * FROM topics WHERE id=?", (topic_id,))
    try:
        frequency = str(form.get("schedule"))
        clock = str(form.get("schedule_time", "09:00"))
        weekday = int(form.get("weekday", 0))
        if frequency not in ("manual", "daily", "weekly") or weekday not in range(7):
            raise ValueError("调度选项无效")
        datetime.strptime(clock, "%H:%M")
        if frequency != "manual":
            raise ValueError("请在关注名单页面设置行业日报的统一更新时段")
        upcoming = next_run(frequency, clock, weekday, settings()["limits"]["timezone"])
        db.execute("UPDATE topics SET schedule=?,schedule_time=?,weekday=?,next_run=? WHERE id=?", (frequency, clock, weekday, upcoming, topic_id))
        return redirect(f"/topics/{topic_id}", "自动研究规则已保存")
    except ValueError as error:
        return redirect(f"/topics/{topic_id}", error=str(error))


@app.post("/topics/{topic_id}/tasks")
def topic_task(topic_id: int, form=Depends(form_data)):
    try:
        kind = str(form.get("kind", "research"))
        if kind not in ("chat", "summary"):
            raise ValueError("任务类型无效")
        prompt = text(form, "prompt", 10000) if kind == "chat" else ""
        brief_id = int(form["brief_id"]) if form.get("brief_id") else None
        task_id = enqueue(topic_id, kind, prompt, brief_id)
        return redirect(f"/tasks/{task_id}")
    except ValueError as error:
        return redirect(f"/topics/{topic_id}", error=str(error))


@app.post("/topics/{topic_id}/observations")
def observation(topic_id: int, form=Depends(form_data)):
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
def brief_page(request: Request, brief_id: int):
    brief = required("SELECT b.*,t.title AS topic_title FROM briefs b JOIN topics t ON t.id=b.topic_id WHERE b.id=?", (brief_id,))
    return page(request, "brief.html", title=brief["title"], brief=brief, sources=db.query("SELECT * FROM sources WHERE task_id=? ORDER BY id", (brief["task_id"],)))


@app.post("/briefs/{brief_id}/save")
def save_brief(brief_id: int, form=Depends(form_data)):
    brief = required("SELECT * FROM briefs WHERE id=?", (brief_id,))
    sources = db.query("SELECT * FROM sources WHERE task_id=? ORDER BY id", (brief["task_id"],))
    appendix = "\n\n## 来源记录\n" + "\n".join(f"- [{source['title']}]({source['url']})；发布日期：{source['published_at'] or '未提供'}；获取时间：{source['fetched_at']}；{source['extent']}" for source in sources)
    artifact_id = save_artifact(brief["topic_id"], brief["title"], brief["body"] + appendix, brief["task_id"])
    return redirect(f"/artifacts/{artifact_id}", "已保存为新的成果版本")


@app.get("/library", response_class=HTMLResponse)
def library(request: Request, q: str = ""):
    q = q.strip()[:200]
    results = db.query("SELECT a.*,t.title AS topic_title FROM artifacts a JOIN topics t ON t.id=a.topic_id WHERE a.title LIKE ? OR a.body LIKE ? ORDER BY a.id DESC LIMIT 100", (f"%{q}%", f"%{q}%"))
    return page(request, "library.html", title="成果库", artifacts=results, q=q)


@app.get("/artifacts/{artifact_id}", response_class=HTMLResponse)
def artifact_page(request: Request, artifact_id: int):
    artifact = required("SELECT a.*,t.title AS topic_title FROM artifacts a JOIN topics t ON t.id=a.topic_id WHERE a.id=?", (artifact_id,))
    return page(request, "artifact.html", title=artifact["title"], artifact=artifact, versions=db.query("SELECT id,title,version,created_at FROM artifacts WHERE topic_id=? ORDER BY id DESC", (artifact["topic_id"],)))


@app.post("/artifacts/{artifact_id}")
def edit_artifact(artifact_id: int, form=Depends(form_data)):
    artifact = required("SELECT * FROM artifacts WHERE id=?", (artifact_id,))
    try:
        new_id = save_artifact(artifact["topic_id"], text(form, "title", 180), text(form, "body", 100000), artifact["origin_task"])
        return redirect(f"/artifacts/{new_id}", "已保存新版本，之前的版本仍可查阅")
    except ValueError as error:
        return redirect(f"/artifacts/{artifact_id}", error=str(error))


@app.get("/artifacts/{artifact_id}/export")
def export_artifact(artifact_id: int):
    artifact = required("SELECT * FROM artifacts WHERE id=?", (artifact_id,))
    body = f"# {artifact['title']}\n\n版本：{artifact['version']}\n保存时间：{artifact['created_at']}\n\n{artifact['body']}\n"
    return PlainTextResponse(body, headers={"Content-Disposition": f'attachment; filename="research-{artifact_id}-v{artifact["version"]}.md"'}, media_type="text/markdown")


@app.get("/tasks", response_class=HTMLResponse)
def task_list(request: Request):
    tasks = db.query("SELECT x.*,t.title AS topic_title FROM tasks x LEFT JOIN topics t ON t.id=x.topic_id ORDER BY x.id DESC LIMIT 100")
    return page(request, "tasks.html", title="研究任务", tasks=tasks, usage=usage_summary(), scheduler_error=db.one("SELECT value FROM runtime WHERE name='scheduler_error'"))


@app.get("/tasks/{task_id}", response_class=HTMLResponse)
def task_page(request: Request, task_id: int):
    task = required("SELECT x.*,t.title AS topic_title FROM tasks x LEFT JOIN topics t ON t.id=x.topic_id WHERE x.id=?", (task_id,))
    return page(request, "task.html", title=KINDS.get(task["kind"], "研究任务"), task=task,
        sources=db.query("SELECT * FROM sources WHERE task_id=? ORDER BY id", (task_id,)),
        brief=db.one("SELECT id FROM briefs WHERE task_id=?", (task_id,)),
        artifact=db.one("SELECT id FROM artifacts WHERE origin_task=? ORDER BY id DESC LIMIT 1", (task_id,)),
        calls=db.query("SELECT * FROM usage WHERE task_id=? ORDER BY id", (task_id,)),
        steps=db.query("SELECT * FROM task_steps WHERE task_id=? ORDER BY id", (task_id,)),
        coverage=db.query("SELECT c.* FROM coverage_checks c JOIN collection_runs r ON r.id=c.run_id WHERE r.task_id=? ORDER BY c.id", (task_id,)),
        documents=db.query("SELECT d.* FROM documents d JOIN document_task_links l ON l.document_id=d.id WHERE l.task_id=? ORDER BY d.id", (task_id,)))


@app.get("/tasks/{task_id}/events")
async def task_events(request: Request, task_id: int):
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
def cancel_task(task_id: int, form=Depends(form_data)):
    required("SELECT id FROM tasks WHERE id=?", (task_id,))
    with db.transaction() as connection:
        connection.execute("UPDATE tasks SET cancel_requested=1 WHERE id=? AND status IN ('queued','running')", (task_id,))
        connection.execute("UPDATE tasks SET status='cancelled',progress='已取消',finished_at=? WHERE id=? AND status='queued'", (db.now(), task_id))
    return redirect(f"/tasks/{task_id}", "已提交取消请求")


@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    return page(request, "settings.html", title="模型与运行设置", usage=usage_summary())


@app.post("/settings/profiles")
def profile_save(form=Depends(form_data)):
    try:
        profile_id = str(form.get("id") or secrets.token_hex(8))
        profile = Profile(id=profile_id, name=text(form, "name", 80), provider=text(form, "provider", 30), base_url=text(form, "base_url", 500), model=text(form, "model", 120), reasoning_effort=str(form.get("reasoning_effort", "low")), input_price=float(form["input_price"]) if form.get("input_price") else None, output_price=float(form["output_price"]) if form.get("output_price") else None, price_note=str(form.get("price_note", "")))
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
        if mode not in ("test_chat", "test_search"):
            raise ValueError("测试类型无效")
        task_id = enqueue(None, mode, profile_id=str(form.get("profile_id")))
        return redirect(f"/tasks/{task_id}")
    except ValueError as error:
        return redirect("/settings", error=str(error))


@app.post("/settings/limits")
def limits_save(form=Depends(form_data)):
    try:
        limits = Limits(daily_requests=form.get("daily_requests"), task_requests=form.get("task_requests"), output_tokens=form.get("output_tokens"), timezone="Asia/Shanghai")
        with config_lock():
            config = settings()
            config["limits"] = limits.model_dump()
            write_json("config.json", config)
        return redirect("/settings", "调用额度已保存，后续任务使用新设置")
    except ValidationError:
        return redirect("/settings", error="额度格式无效，请填写页面允许范围内的整数")


@app.get("/health")
def health():
    db.one("SELECT 1 AS ok")
    return {"status": "ok", "version": __version__, "worker": "running" if worker_alive() else "stopped"}


from .station_web import register

register(app, page, form_data, redirect, required, text)
