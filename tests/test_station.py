import pytest
from pydantic import ValidationError

from researcher import db, station
from researcher.pipeline import Extraction, TopicAssessment, store_extraction
from researcher.providers import ResearchError, Source
from researcher.service import context


def test_event_sources_aggregate_and_distinct_milestones(topic):
    release = Source(
        "Hailo 发布支持边缘生成式 AI 的 Hailo-10H 芯片",
        "https://ai.zhiding.cn/2025/0723/3169328.shtml",
        "人工智能芯片初创公司Hailo Technologies今日宣布推出第二代AI加速器Hailo-10H",
        "网页内容（服务商提取）",
    )
    commercial = Source(
        "Hailo 宣布全面推出 Hailo-10H 边缘人工智能加速器",
        "https://www.geekpark.net/news/351800",
        "Hailo-10H 现已正式投入商用。",
        "网页内容（服务商提取）",
    )
    document_a, version_a = station.save_document(release, None)
    document_b, version_b = station.save_document(commercial, None)
    claim_a = station.save_claim(document_a, version_a, "资料报道 Hailo 推出第二代加速器 Hailo-10H", "宣布推出第二代AI加速器Hailo-10H", "待核实")
    claim_b = station.save_claim(document_b, version_b, "资料报道 Hailo-10H 正式商用", "Hailo-10H 现已正式投入商用", "待核实")
    company_id = station.entity("厂商", "Hailo")
    product_id = station.entity("产品", "Hailo-10H")
    release_event = {"identity_key": "Hailo|Hailo-10H|推出|2025", "event_type": "产品发布", "title": "Hailo-10H 发布", "what_happened": "资料报道 Hailo 推出 Hailo-10H", "why_important": "端侧生成式 AI 产品继续增加", "importance": 2, "occurred_at": "2025-07-23T00:00:00+00:00"}
    first, created = station.add_event(release_event, [claim_a], [company_id, product_id], [topic["id"]])
    assert created
    repeated, created = station.add_event(release_event, [claim_b], [company_id], [topic["id"]])
    assert repeated == first and not created
    assert db.one("SELECT count(*) AS n FROM event_claims WHERE event_id=?", (first,))["n"] == 2
    renamed = dict(release_event, identity_key="Hailo|Hailo-10H|产品上市|2025-07-23", title="Hailo-10H 产品上市")
    renamed_id, created = station.add_event(renamed, [claim_a], [company_id, product_id], [topic["id"]])
    assert renamed_id == first and not created
    assert db.one("SELECT event_id FROM event_aliases WHERE identity_key=?", (station.normalized(renamed["identity_key"]),))["event_id"] == first
    commercial_event = dict(release_event, identity_key="Hailo|Hailo-10H|商用|2025", title="Hailo-10H 商用", event_type="商业进展")
    second, created = station.add_event(commercial_event, [claim_b], [company_id], [topic["id"]])
    assert created and second != first
    assert db.one("SELECT count(*) AS n FROM events")["n"] == 2


def test_claim_excerpt_and_judgment_history(client, csrf, topic):
    source = Source("Hailo 公司资料", "https://hailo.ai/files/hailo-company-brochure-zhs/", "超300个客户项目\n第二代批量生产", "网页内容（服务商提取）")
    document_id, version_id = station.save_document(source, None)
    with pytest.raises(ResearchError):
        station.save_claim(document_id, version_id, "无法核对的厂商数字", "不存在的原文摘录", "厂商宣称")
    first_claim = station.save_claim(document_id, version_id, "Hailo 公司资料宣称超 300 个客户项目", "超300个客户项目", "厂商宣称")
    second_claim = station.save_claim(document_id, version_id, "Hailo 公司资料提及第二代批量生产", "第二代批量生产", "厂商宣称")
    first, changed = station.revise(topic["id"], "行业竞争", "公司资料宣称客户项目数超过 300 个。", "待核实", "仅代表厂商自己的项目口径", "项目定义尚未核验", "首次记录厂商资料中的客户项目表述", None, [first_claim], [])
    assert changed
    second, changed = station.revise(topic["id"], "行业竞争", "公司资料同时提及第二代产品批量生产。", "需要修正", "厂商资料未提供独立审计", "具体出货时间尚未核验", "新增第二代批量生产的厂商表述", None, [second_claim], [first_claim])
    assert changed and second != first
    assert db.one("SELECT count(*) AS n FROM judgment_versions")["n"] == 2
    assert {item["stance"] for item in db.query("SELECT stance FROM judgment_evidence WHERE version_id=?", (second,))} == {"支持", "反对"}
    response = client.post(f"/claims/{second_claim}/verify", data={"csrf": csrf, "verification": "人工核对发现不符"})
    assert response.status_code == 200
    assert db.one("SELECT verification,verified_at FROM claims WHERE id=?", (second_claim,))["verified_at"]
    assert db.one("SELECT status FROM judgments WHERE topic_id=? AND purpose='行业竞争'", (topic["id"],))["status"] == "存在争议"
    assert db.one("SELECT count(*) AS n FROM judgment_versions")["n"] == 3


def test_personal_annotation_is_separate_and_comparison_needs_conditions(client, csrf, topic):
    body = "希望核查软件迁移如何影响产品竞争力。"
    client.post("/annotations", data={"csrf": csrf, "target_kind": "topic", "target_id": topic["id"], "body": body})
    assert body not in context(topic["id"])
    assert db.one("SELECT body FROM annotations WHERE target_kind='topic' AND target_id=?", (topic["id"],))["body"] == body
    first = station.entity("产品", "Hailo-8")
    second = station.entity("产品", "Hailo-10H")
    response = client.get("/compare", params=[("id", first), ("id", second)])
    assert response.status_code == 200
    assert "测试条件不完整或不一致" in response.text


def test_deployment_question_waits_for_scenario_and_note_has_versions(client, csrf, topic):
    body = "希望核查软件迁移如何影响产品竞争力。"
    client.post("/annotations", data={"csrf": csrf, "target_kind": "topic", "target_id": topic["id"], "body": body})
    note = db.one("SELECT * FROM annotations ORDER BY id DESC LIMIT 1")
    response = client.post("/questions", data={
        "csrf": csrf, "target_kind": "topic", "target_id": topic["id"],
        "purpose": "部署决策", "main_question": "软件迁移如何影响端侧推理加速器的部署？",
        "completion": "列出有依据的软件迁移约束和缺失的测试条件。", "note_id": str(note["id"]),
    })
    assert response.status_code == 200
    question = db.one("SELECT * FROM research_questions ORDER BY id DESC LIMIT 1")
    assert question["status"] == "needs_input" and question["task_id"] is None
    assert station.decode(question["selected_note_ids_json"]) == [note["id"]]
    assert "需要补充应用场景" in response.text
    edited = body + "关注功耗条件。"
    client.post(f"/annotations/{note['id']}", data={"csrf": csrf, "body": edited})
    assert db.one("SELECT count(*) AS n FROM annotation_versions WHERE annotation_id=?", (note["id"],))["n"] == 2
    assert body not in context(topic["id"])


def test_comparison_rejects_more_than_three_products(client):
    names = ["Hailo-8", "Hailo-10H", "BrainChip AKD1500", "Axelera Metis"]
    ids = [station.entity("产品", name) for name in names]
    response = client.get("/compare", params=[("id", identifier) for identifier in ids])
    assert response.status_code == 200
    assert "一次最多选择三款产品" in response.text


def test_numeric_model_reference_is_normalized():
    item = Extraction.model_validate({"claims": [{"ref": 1, "passage_id": "d17v1p0", "statement": "Hailo 公司资料宣称超 300 个客户项目", "label": "厂商宣称"}], "events": [], "fields": [], "relations": [], "questions": []})
    assert item.claims[0].ref == "1"


def test_topic_update_requires_one_judgment_per_purpose():
    revision = {"topic_id": 2, "purpose": "行业竞争", "conclusion": "Hailo 的软件套件提供模型编译和运行时等组件。", "status": "得到加强", "reason": "本次补充软件工具链相关的公开资料。", "event_id": 3, "supporting_claim_ids": [1]}
    with pytest.raises(ValidationError):
        TopicAssessment.model_validate({"industry": [revision, revision], "deployment": None, "questions": []})


def test_event_date_must_be_supported_by_cited_source(topic):
    text = "时间：2026-10-29 13:16\n贸泽电子即日起开售NXP Semiconductors的全新Ara240独立神经处理单元。"
    source = Source("贸泽开售 NXP Ara240", "https://www.eepw.com.cn/article/202610/484309.htm", text, "网页内容（服务商提取）", "2026-10-28T16:00:00+00:00")
    document_id, version_id = station.save_document(source, None)
    found = {source.url: (document_id, version_id)}
    passages = {"ara240-date": {"document_id": document_id, "text": text}}
    value = {"claims": [{"ref": "c1", "passage_id": "ara240-date", "statement": "贸泽电子开售 NXP Ara240 独立神经处理单元。", "label": "厂商宣称"}], "events": [{"identity_key": "nxp|ara240|产品发布|2026-10-29", "event_type": "产品发布", "title": "NXP Ara240 独立神经处理单元上市", "what_happened": "贸泽电子宣布开售 Ara240 独立神经处理单元。", "why_important": "公开销售渠道为边缘推理部署提供了新的候选产品。", "occurred_at": "2026-10-28", "claim_refs": ["c1"], "entities": [{"kind": "产品", "name": "Ara240"}], "topic_ids": [topic["id"]]}], "questions": []}
    with pytest.raises(ResearchError, match="发生日期缺少"):
        store_extraction(Extraction.model_validate(value), found, {"entities": [], "topics": [topic]}, passages)
    assert not db.query("SELECT * FROM claims")
    value["events"][0]["occurred_at"] = "2026-10-29"
    event_ids, new_ids, claim_ids = store_extraction(Extraction.model_validate(value), found, {"entities": [], "topics": [topic]}, passages)
    assert len(event_ids) == len(new_ids) == len(claim_ids) == 1
    assert db.one("SELECT occurred_at FROM events WHERE id=?", (event_ids[0],))["occurred_at"] == "2026-10-29"
