from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from pydantic import ValidationError

from . import continuity, db, research, station
from .config import ResearchSettings, config_lock, settings, write_json
from .web_ids import RecordId, parse_record_id


def register(app, page, form_data, redirect, required, text):
    router = APIRouter()

    @router.get("/research", response_class=HTMLResponse)
    def research_home(request: Request, q: str = ""):
        sessions = db.query("SELECT s.*,(SELECT id FROM tasks WHERE session_id=s.id ORDER BY id DESC LIMIT 1) AS task_id FROM research_sessions s WHERE s.title LIKE ? ORDER BY s.updated_at DESC LIMIT 60", ("%" + q[:200] + "%",))
        return page(request, "research.html", title="探索研究", sessions=sessions, q=q, notes=db.query("SELECT id,body FROM annotations WHERE archived=0 ORDER BY id DESC LIMIT 50"), gaps=db.query("SELECT * FROM research_gaps WHERE status!='resolved' ORDER BY priority DESC,updated_at DESC LIMIT 30"), knowledge_count=db.one("SELECT count(*) AS n FROM knowledge_items WHERE status='supported'")["n"])

    def job_from_form(form):
        purpose = str(form.get("purpose", "行业竞争"))
        if purpose not in station.PURPOSES:
            raise ValueError("请选择有效的研究用途")
        notes = [parse_record_id(value) for value in form.getlist("note_id")]
        for identifier in notes:
            if not db.one("SELECT id FROM annotations WHERE id=? AND archived=0", (identifier,)):
                raise ValueError("选中的批注不存在")
        return {"main_question": text(form, "message", 5000), "purpose": purpose, "completion": str(form.get("completion", "形成有原文依据的结论、适用条件和仍待核实的问题"))[:1000], "conditions": {"scenario": str(form["scenario"])[:500]} if form.get("scenario") else {}, "selected_note_ids": notes, "force_refresh": form.get("force_refresh") == "yes"}

    @router.post("/research")
    def create_research(form=Depends(form_data)):
        try:
            task_id = research.enqueue("interactive", job_from_form(form))
            task = db.one("SELECT session_id FROM tasks WHERE id=?", (task_id,))
            return redirect(f"/research/{task['session_id']}")
        except ValueError as error:
            return redirect("/research", error=str(error))

    @router.get("/research/{session_id}", response_class=HTMLResponse)
    def session_page(request: Request, session_id: RecordId):
        session = required("SELECT * FROM research_sessions WHERE id=?", (session_id,))
        task = db.one("SELECT * FROM tasks WHERE session_id=? ORDER BY id DESC LIMIT 1", (session_id,))
        result = station.decode(task["result_json"]) if task else {}
        identifiers = result.get("knowledge_ids", [])
        knowledge = list({item["id"]: item for item in (continuity.canonical_knowledge(identifier) for identifier in identifiers) if item}.values())
        return page(request, "research_session.html", title=session["title"], session=session, task=task, result=result, knowledge=knowledge, messages=db.query("SELECT * FROM research_messages WHERE session_id=? ORDER BY id", (session_id,)), steps=db.query("SELECT * FROM task_steps WHERE task_id=? ORDER BY id", (task["id"],)) if task else [], calls=db.query("SELECT count(*) AS n,sum(input_tokens) AS inputs,sum(output_tokens) AS outputs FROM usage WHERE task_id=?", (task["id"],))[0] if task else {}, notes=db.query("SELECT id,body FROM annotations WHERE archived=0 ORDER BY id DESC LIMIT 50"), feedback=db.query("SELECT * FROM research_feedback WHERE session_id=? ORDER BY id DESC", (session_id,)), gaps=db.query("SELECT * FROM research_gaps WHERE origin_task_id IN (SELECT id FROM tasks WHERE session_id=?) ORDER BY priority DESC,id", (session_id,)))

    @router.post("/research/{session_id}/messages")
    def continue_research(session_id: RecordId, form=Depends(form_data)):
        session = required("SELECT * FROM research_sessions WHERE id=?", (session_id,))
        try:
            job = job_from_form(form)
            job["scope"] = station.decode(session["scope_json"])
            previous = db.one("SELECT job_json,context_brief FROM tasks WHERE session_id=? ORDER BY id DESC LIMIT 1", (session_id,))
            if previous:
                target = station.decode(previous["job_json"])
                if target.get("target_kind") in {"event", "topic", "entity"}:
                    job.update(target_kind=target["target_kind"], target_id=target["target_id"])
                if target.get("topic_id"):
                    job["topic_id"] = target["topic_id"]
            research.enqueue("interactive", job, session_id=session_id, brief_id=previous["context_brief"] if previous else None)
            return redirect(f"/research/{session_id}")
        except ValueError as error:
            return redirect(f"/research/{session_id}", error=str(error))

    @router.post("/research/{session_id}/feedback")
    def give_feedback(session_id: RecordId, form=Depends(form_data)):
        required("SELECT id FROM research_sessions WHERE id=?", (session_id,))
        try:
            item_id = parse_record_id(form["item_id"]) if form.get("item_id") else None
            research.feedback(session_id, parse_record_id(form.get("task_id")), str(form.get("kind", "")), str(form.get("body", ""))[:2000], item_id)
            return redirect(f"/research/{session_id}", "反馈已保存，相关知识的核验状态已更新")
        except (ValueError, TypeError) as error:
            return redirect(f"/research/{session_id}", error=str(error))

    @router.post("/research/gaps/{gap_id}/start")
    def start_gap(gap_id: RecordId, form=Depends(form_data)):
        required("SELECT id FROM research_gaps WHERE id=?", (gap_id,))
        try:
            with db.transaction() as connection:
                gap = connection.execute("SELECT * FROM research_gaps WHERE id=?", (gap_id,)).fetchone()
                task_id = research.enqueue_gap(gap, connection)
            task = db.one("SELECT session_id FROM tasks WHERE id=?", (task_id,))
            return redirect(f"/research/{task['session_id']}")
        except ValueError as error:
            return redirect("/research", error=str(error))

    @router.post("/research/gaps/{gap_id}/pause")
    def pause_gap(gap_id: RecordId, form=Depends(form_data)):
        required("SELECT id FROM research_gaps WHERE id=?", (gap_id,))
        with db.transaction() as connection:
            gap = connection.execute("SELECT task_id FROM research_gaps WHERE id=?", (gap_id,)).fetchone()
            connection.execute("UPDATE research_gaps SET status='paused',updated_at=? WHERE id=?", (db.now(), gap_id))
            task = connection.execute("SELECT status,session_id FROM tasks WHERE id=?", (gap["task_id"],)).fetchone()
            if task and task["status"] == "running":
                connection.execute("UPDATE tasks SET cancel_requested=1 WHERE id=?", (gap["task_id"],))
            elif task and task["status"] == "queued":
                connection.execute("UPDATE tasks SET status='cancelled',finished_at=?,progress='已暂停' WHERE id=?", (db.now(), gap["task_id"]))
                connection.execute("UPDATE research_sessions SET status='cancelled',updated_at=? WHERE id=?", (db.now(), task["session_id"]))
        return redirect("/research", "该后续问题已暂停")

    @router.post("/settings/research")
    def save_research_settings(form=Depends(form_data)):
        try:
            previous = settings()["research"]
            values = {key: form.get(key, value) for key, value in previous.items()}
            if "event_research_setting" in form:
                values["automatic_event_research"] = form.get("automatic_event_research") == "yes"
            config = ResearchSettings(**values)
            with config_lock():
                saved = settings()
                saved["research"] = config.model_dump()
                write_json("config.json", saved)
            return redirect("/settings#research", "研究额度与本地检索设置已保存")
        except ValidationError:
            return redirect("/settings#research", error="研究设置格式无效，请检查允许范围")

    app.include_router(router)
