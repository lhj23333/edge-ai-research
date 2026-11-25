import pytest
from lxml import html
from pydantic import ValidationError

from researcher import db, station
from researcher.research_engine import save_research_event
from researcher.research_models import Draft, ExtractEvent
from researcher.providers import ResearchError, Source


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
    renamed_id, created = station.add_event(renamed, [claim_a], [company_id, product_id], [topic["id"]], existing_event_id=first)
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


def test_manual_update_binds_claim_to_saved_excerpt_and_preserves_legacy_claim(client, csrf):
    url = "https://www.raspberrypi.com/news/citizen-science-with-raspberry-pi/"
    title = "Citizen science with Raspberry Pi"
    excerpt = "This Maker Monday, we look at ways environmentalists are using Raspberry Pi to understand the world around them."
    document, old_version = station.save_document(Source(title, url, excerpt, "用户提供摘录"), None)
    legacy = station.save_claim(document, old_version, "Raspberry Pi 展示环境监测项目。", excerpt, "待核实")
    form = {"csrf": csrf, "title": title, "what_happened": "Raspberry Pi 介绍环境监测中的应用。", "why_important": "记录公开的边缘设备应用。",
            "company": "Raspberry Pi", "event_type": "其他", "url": url, "excerpt": excerpt}
    response = client.post("/updates/manual", data=form, follow_redirects=False)
    assert response.status_code == 303 and response.headers["location"].startswith("/events/")
    claim = db.one("SELECT id,document_version_id,label FROM claims WHERE statement=?", (form["what_happened"],))
    assert claim["document_version_id"] == old_version and claim["label"] == "待核实"
    evidence, = db.query("SELECT ch.id,ch.version_id,ch.content,ch.start_offset,ch.end_offset,ce.verdict FROM claim_evidence ce "
                         "JOIN document_chunks ch ON ch.id=ce.chunk_id JOIN document_versions v ON v.id=ch.version_id "
                         "WHERE ce.claim_id=? AND v.document_id=?", (claim["id"], document))
    assert evidence == {"id": evidence["id"], "version_id": old_version, "content": excerpt,
                        "start_offset": 0, "end_offset": len(excerpt), "verdict": "insufficient"}
    assert not db.query("SELECT chunk_id FROM claim_evidence WHERE claim_id=?", (legacy,))
    assert db.one("SELECT document_version_id FROM claims WHERE id=?", (legacy,))["document_version_id"] == old_version
    event = client.get(response.headers["location"])
    path = f"/documents/{document}?version={old_version}"
    link = f"{path}#chunk-{evidence['id']}"
    assert link in html.fromstring(event.content).xpath("//a/@href")
    source = client.get(path)
    assert source.status_code == 200 and "仅记录资料版本，暂无原文片段关联" in source.text
    assert html.fromstring(source.content).xpath(f"//*[@id='chunk-{evidence['id']}']")
    response = client.post("/updates/manual", data=form, follow_redirects=False)
    assert response.status_code == 303
    assert db.query("SELECT chunk_id FROM claim_evidence WHERE claim_id=?", (claim["id"],)) == [{"chunk_id": evidence["id"]}]


def test_personal_annotation_is_separate_and_comparison_needs_conditions(client, csrf, topic):
    body = "希望核查软件迁移如何影响产品竞争力。"
    client.post("/annotations", data={"csrf": csrf, "target_kind": "topic", "target_id": topic["id"], "body": body})
    assert not db.query("SELECT id FROM claims WHERE statement=?", (body,))
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
    assert not db.query("SELECT id FROM claims WHERE statement=?", (body,))


def test_comparison_rejects_more_than_three_products(client):
    names = ["Hailo-8", "Hailo-10H", "BrainChip AKD1500", "Axelera Metis"]
    ids = [station.entity("产品", name) for name in names]
    response = client.get("/compare", params=[("id", identifier) for identifier in ids])
    assert response.status_code == 200
    assert "一次最多选择三款产品" in response.text


def test_comparison_excludes_non_products_archived_and_missing_ids(client):
    manufacturer = station.entity("厂商", "Hailo")
    first = station.entity("产品", "Hailo-8")
    second = station.entity("产品", "Hailo-10H")
    archived = station.entity("产品", "Hailo-8R")
    db.execute("UPDATE entities SET state='archived' WHERE id=?", (archived,))
    response = client.get("/compare", params=[("id", identifier) for identifier in (manufacturer, first, second, archived, 999999, second)])
    assert response.status_code == 200
    assert [row["id"] for row in response.context["selected"]] == [second, first]
    assert "2 款产品" in response.text
    assert "一次最多选择三款产品" not in response.text


def test_product_aliases_merge_evidence_and_count_as_one_comparison_slot(client, csrf):
    nxp = station.entity("厂商", "NXP Semiconductors")
    ara = station.entity("产品", "Ara240", parent_id=nxp)
    assert station.entity("产品", "Ara240 DNPU", parent_id=nxp) == ara
    dupe = station.entity("产品", "Ara240 独立神经处理单元", parent_id=nxp)
    hailo = station.entity("厂商", "Hailo")
    older = station.entity("产品", "Hailo-8", parent_id=hailo)
    newer = station.entity("产品", "Hailo-10H", parent_id=hailo)
    assert station.entity("产品", "Hailo 10H", parent_id=hailo) == newer != older
    deepx = station.entity("厂商", "DEEPX")
    dx_m1 = station.entity("产品", "DX-M1", parent_id=deepx)
    assert station.entity("产品", "DX M1 Chip", parent_id=deepx) == dx_m1
    assert station.entity("产品", "DX-M2", parent_id=deepx) != dx_m1
    assert "代号不同" in client.post(f"/entities/{older}/merge", data={"csrf": csrf, "target_id": newer}).text
    assert "归属厂商不同" in client.post(f"/entities/{newer}/merge", data={"csrf": csrf, "target_id": ara}).text
    url = "https://www.eepw.com.cn/article/202610/484309.htm"
    statement = "贸泽电子即日起开售NXP Semiconductors的全新Ara240独立神经处理单元。"
    document, version = station.save_document(Source("贸泽开售 NXP Ara240", url, statement, "公开资料"), None)
    claim = station.save_claim(document, version, statement, statement, "公开披露")
    claim_alias = station.save_claim(document, version, "Ara240 是独立神经处理单元。", statement, "公开披露")
    with db.transaction() as connection:
        for identifier, claim_id in ((ara, claim), (dupe, claim_alias)):
            connection.execute("INSERT INTO entity_fields(entity_id,field_name,value,claim_id,updated_at) VALUES(?,'业务定位','独立神经处理单元',?,?)",
                               (identifier, claim_id, db.now()))
            connection.execute("INSERT INTO relations(from_entity_id,to_entity_id,relation_type,explanation,claim_id,created_at) "
                               "VALUES(?,?,'提供产品',?,?,?)", (nxp, identifier, statement, claim_id, db.now()))
    event, _ = station.add_event({"identity_key": "nxp-ara240-availability", "event_type": "产品发布", "title": "NXP Ara240 上市",
                                  "what_happened": statement, "why_important": "新增可购买的部署候选。"}, [claim], [dupe, ara], [])
    assert "归并重复产品" in client.get(f"/entities/{dupe}").text
    response = client.post(f"/entities/{dupe}/merge", data={"csrf": csrf, "target_id": ara}, follow_redirects=False)
    assert response.headers["location"].startswith(f"/entities/{ara}?")
    assert client.get(f"/entities/{dupe}", follow_redirects=False).headers["location"].startswith(f"/entities/{ara}?")
    assert station.entity("产品", "Ara240 独立神经处理单元") == ara
    assert db.query("SELECT entity_id,feed_only FROM event_entities WHERE event_id=?", (event,)) == [{"entity_id": ara, "feed_only": 0}]
    assert db.query("SELECT entity_id FROM entity_fields") == [{"entity_id": ara}]
    assert db.query("SELECT to_entity_id FROM relations") == [{"to_entity_id": ara}]
    assert db.query("SELECT entity_id,claim_id FROM entity_claims") == [{"entity_id": ara, "claim_id": claim_alias}]
    view = client.get(f"/entities/{ara}")
    assert "Ara240 DNPU" in view.text and "归并保留的证据" in view.text
    assert f'/documents/{document}?version={version}' in view.text
    result = client.get("/compare", params=[("id", identifier) for identifier in (ara, dupe, ara, older, newer)])
    assert {item["id"] for item in result.context["selected"]} == {ara, older, newer}
    assert "已选择 3 / 3" in result.text and "一次最多选择三款产品" not in result.text
    assert not html.fromstring(result.content).xpath(f"//input[@name='id'][@value='{dupe}']")


@pytest.mark.parametrize(("first", "second", "comparable"), [
    ("典型", "典型", False),
    ("模型 ResNet-50；INT8；批量 1；设备 Raspberry Pi 5", "模型 ResNet-50；INT8；批量 1；设备 Raspberry Pi 5", False),
    ("模型 ResNet-50；INT8；批量 1；功耗 5W；设备 Raspberry Pi 5", "模型 ResNet-50；INT8；批量 2；功耗 5W；设备 Raspberry Pi 5", False),
    ("模型 ResNet-50；INT8；批量 1；功耗 5W；设备 Raspberry Pi 5", "模型 ResNet-50；INT8；批量 1；功耗 5W；设备 Raspberry Pi 5", True),
])
def test_comparison_requires_complete_and_matching_dimensions(client, first, second, comparable):
    products = [station.entity("产品", name) for name in ("Hailo-8", "Hailo-10H")]
    for identifier, value in zip(products, (first, second)):
        db.execute("INSERT INTO entity_fields(entity_id,field_name,value,updated_at) VALUES(?,'测试条件',?,?)",
                   (identifier, value, db.now()))
    response = client.get("/compare", params=[("id", identifier) for identifier in products])
    assert response.context["comparable"] is comparable
    assert ("暂不能直接排名" in response.text) is not comparable


def test_topic_update_requires_one_judgment_per_purpose():
    revision = {"topic_id": 2, "purpose": "行业竞争", "conclusion": "Hailo 的软件套件提供模型编译和运行时等组件。", "status": "得到加强", "reason": "本次补充软件工具链相关的公开资料。", "supporting_refs": ["f1"]}
    with pytest.raises(ValidationError):
        Draft.model_validate({"judgments": [revision, revision], "completion_notes": "已检查软件条件和适用范围。", "complete": True})


def test_event_date_must_be_supported_by_cited_source(topic):
    text = "时间：2026-10-29 13:16\n贸泽电子即日起开售NXP Semiconductors的全新Ara240独立神经处理单元。"
    source = Source("贸泽开售 NXP Ara240", "https://www.eepw.com.cn/article/202610/484309.htm", text, "网页内容（服务商提取）", "2026-10-28T16:00:00+00:00")
    document_id, version_id = station.save_document(source, None)
    claim_id = station.save_claim(document_id, version_id, "贸泽电子开售 NXP Ara240 独立神经处理单元。", text, "厂商宣称")
    scope = {"entities": [], "topics": [topic]}
    value = {"identity_key": "nxp|ara240|产品发布|2026-10-29", "event_type": "产品发布", "title": "NXP Ara240 独立神经处理单元上市", "what_happened": "贸泽电子宣布开售 Ara240 独立神经处理单元。", "why_important": "公开销售渠道为边缘推理部署提供了新的候选产品。", "occurred_at": "2026-10-28", "claim_refs": [1], "entities": [{"kind": "产品", "name": "Ara240"}], "topic_ids": [topic["id"]]}
    with pytest.raises(ResearchError, match="发生日期缺少"):
        save_research_event(ExtractEvent.model_validate(value), [{"content": text}], [claim_id], scope)
    assert not db.query("SELECT * FROM events")
    value["occurred_at"] = "2026-10-29"
    event_id, created = save_research_event(ExtractEvent.model_validate(value), [{"content": text}], [claim_id], scope)
    assert created
    assert db.one("SELECT occurred_at FROM events WHERE id=?", (event_id,))["occurred_at"] == "2026-10-29"
