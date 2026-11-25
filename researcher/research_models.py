from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .continuity import digest
from .industry import prompt

RULES = prompt("deep_rules")
PLANNING = RULES + "\n" + prompt("deep_plan")
SYNTHESIS = RULES + "\n" + prompt("deep_write")
REVIEW = RULES + "\n" + prompt("deep_review")
PROMPT_VERSION = "research-" + digest([PLANNING, SYNTHESIS, REVIEW])[:16]
LEGACY_PROMPT_VERSION = "research-2026-11-06-v4"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ExtractEntity(StrictModel):
    kind: Literal["厂商", "产品", "应用场景", "技术路线", "软件生态"]
    name: str = Field(min_length=2, max_length=120)


class ExtractEvent(StrictModel):
    identity_key: str = Field(min_length=8, max_length=240)
    existing_event_id: int | None = Field(default=None, ge=1)
    version: str = Field(default="", max_length=80, description="资料明确提到的产品或软件版本")
    event_type: str = Field(min_length=1, max_length=80)
    title: str = Field(min_length=8, max_length=180)
    what_happened: str = Field(min_length=12, max_length=700)
    why_important: str = Field(min_length=12, max_length=700)
    competitive_impact: str = Field(max_length=700, default="")
    occurred_at: str | None = None
    importance: int = Field(ge=1, le=5, default=1)
    background: bool = False
    claim_refs: list[str] = Field(min_length=1, max_length=5)
    entities: list[ExtractEntity] = Field(min_length=1, max_length=5)
    topic_ids: list[int] = Field(max_length=5, default_factory=list)

    @field_validator("claim_refs", mode="before")
    @classmethod
    def reference_texts(cls, value):
        return [str(item) for item in value] if isinstance(value, list) else value


class Finding(StrictModel):
    ref: str = Field(min_length=1, max_length=30)
    statement: str = Field(min_length=8, max_length=800)
    label: Literal["公开披露", "厂商宣称", "分析判断", "待核实"]
    chunk_ids: list[int] = Field(min_length=1, max_length=6)
    conditions: dict[str, str] = Field(default_factory=dict, description="影响事实适用范围的产品、版本、测试和部署条件；不包含来源名称或资料发布日期")
    kind: Literal["事实", "术语", "部署条件", "软件条件", "商业进展"] = "事实"


class Point(StrictModel):
    category: Literal["来源陈述", "分析判断", "适用边界"]
    text: str = Field(min_length=8, max_length=1000)
    finding_refs: list[str] = Field(min_length=1, max_length=8)


class JudgmentProposal(StrictModel):
    topic_id: int
    purpose: Literal["行业竞争", "部署决策"]
    conclusion: str = Field(min_length=10, max_length=800)
    status: Literal["初步判断", "得到加强", "需要修正", "存在争议", "待核实"]
    boundaries: str = Field(default="", max_length=800)
    unresolved: str = Field(default="", max_length=800)
    reason: str = Field(min_length=8, max_length=600)
    supporting_refs: list[str] = Field(default_factory=list, max_length=12)
    opposing_refs: list[str] = Field(default_factory=list, max_length=12)


class FieldProposal(StrictModel):
    entity: ExtractEntity
    field_name: Literal["业务定位", "目标客户", "收费", "渠道", "伙伴", "商业进展", "竞争观察", "性能", "模型覆盖", "功耗成本", "软件迁移", "测试条件"]
    value: str = Field(min_length=2, max_length=800)
    finding_ref: str


class RelationProposal(StrictModel):
    source: ExtractEntity
    target: ExtractEntity
    relation_type: Literal["提供产品", "面向场景", "采用技术", "依赖软件", "竞争于", "合作于"]
    explanation: str = Field(min_length=8, max_length=500)
    finding_ref: str


class WatchProposal(StrictModel):
    kind: Literal["company", "topic"]
    name: str = Field(min_length=2, max_length=120)
    reason: str = Field(min_length=8, max_length=500)
    question: str = Field(default="", max_length=500)
    finding_ref: str


class Gap(StrictModel):
    question: str = Field(min_length=8, max_length=500)
    reason: str = Field(min_length=8, max_length=600)
    completion: str = Field(min_length=8, max_length=600)
    priority: int = Field(ge=1, le=5)
    target_kind: Literal["domain", "topic", "entity"] = "domain"
    target_id: int | None = None
    finding_refs: list[str] = Field(default_factory=list, max_length=8)
    existing_gap_id: int | None = None


class Draft(StrictModel):
    findings: list[Finding] = Field(default_factory=list, max_length=40)
    points: list[Point] = Field(default_factory=list, max_length=12)
    events: list[ExtractEvent] = Field(default_factory=list, max_length=12)
    judgments: list[JudgmentProposal] = Field(default_factory=list, max_length=12)
    fields: list[FieldProposal] = Field(default_factory=list, max_length=20)
    relations: list[RelationProposal] = Field(default_factory=list, max_length=20)
    proposals: list[WatchProposal] = Field(default_factory=list, max_length=9)
    gaps: list[Gap] = Field(default_factory=list, max_length=8)
    uncertainties: list[str] = Field(default_factory=list, max_length=20)
    completion_notes: str = Field(min_length=8, max_length=1000)
    complete: bool

    @model_validator(mode="after")
    def one_judgment_per_purpose(self):
        keys = {(item.topic_id, item.purpose) for item in self.judgments}
        if len(keys) != len(self.judgments):
            raise ValueError("同一专题每种用途只提交一项综合判断")
        return self


class DraftSubmission(StrictModel):
    draft: Draft


class FindingReview(StrictModel):
    ref: str
    verdict: Literal["supported", "partial", "contradicted", "insufficient"]
    reason: str = Field(min_length=5, max_length=600)
    existing_knowledge_id: int | None = None
    change_type: Literal["new", "existing", "updated", "conflict"] = "new"
    dedup_reason: str = Field(default="", max_length=600)
    revalidated: bool = False
    supported_statement: str = Field(default="", max_length=800)


class PointReview(StrictModel):
    index: int = Field(ge=0)
    verdict: Literal["supported", "partial", "contradicted", "insufficient"]
    reason: str = Field(min_length=5, max_length=600)
    existing_knowledge_id: int | None = None


class UpdateReview(StrictModel):
    category: Literal["events", "judgments", "fields", "relations", "proposals", "gaps"]
    index: int = Field(ge=0)
    verdict: Literal["supported", "partial", "contradicted", "insufficient"]
    reason: str = Field(min_length=5, max_length=600)
    has_change: bool = True


class Review(StrictModel):
    findings: list[FindingReview]
    points: list[PointReview]
    updates: list[UpdateReview] = Field(default_factory=list)
    rejected_updates: list[str] = Field(default_factory=list)
    uncertainties: list[str] = Field(default_factory=list, max_length=20)
    completion_satisfied: bool
    completion_notes: str = Field(default="", max_length=1000)
    deferred_refs: list[str] = Field(default_factory=list)


class EvidenceReview(StrictModel):
    findings: list[FindingReview]
    points: list[PointReview]
    updates: list[UpdateReview]


class CompletionReview(StrictModel):
    complete: bool
    notes: str = Field(min_length=8, max_length=1000)
    uncertainties: list[str] = Field(default_factory=list, max_length=20)


class KnowledgeQuery(StrictModel):
    query: str = Field(min_length=2, max_length=1000)
    include_history: bool = False


class WebQuery(StrictModel):
    query: str = Field(min_length=2, max_length=128)
    reuse_query_key: str | None = None


class LegacyWebQuery(WebQuery):
    targets: list[str] = Field(default_factory=list, max_length=20)


class ReadSource(StrictModel):
    url: str = Field(min_length=8, max_length=8192)


class ReadEvidence(StrictModel):
    chunk_ids: list[int] = Field(min_length=1, max_length=12)


class InspectTopic(StrictModel):
    topic_id: int


class Finish(StrictModel):
    reason: str = Field(min_length=5, max_length=600)
    complete: bool
    needs_input: str = Field(default="", max_length=1000)


TOOLS = {
    "search_knowledge": (KnowledgeQuery, "检索本机保存的完整资料、历史证据及相关原文；返回带 chunk_id 的片段。"),
    "search_web": (WebQuery, "检索公开网络资料。使用明确查询，可含 site:、after:、before: 和英文产品名。每次检索消耗一次云端请求。"),
    "read_source": (ReadSource, "读取原始网页、文本或文本型 PDF 并保存原文。remaining_web_pages 为 0 时，只能读取 reserved_web_urls 或本次前序搜索已计数的链接，例如尚待补全正文的摘要。"),
    "read_evidence": (ReadEvidence, "按给定 chunk_id 读取原文，检查详细条件或已存证据。"),
    "inspect_topic": (InspectTopic, "读取已有专题判断、支持和反对证据、相关事件与问题。"),
    "finish_research": (Finish, "证据足够或继续调查价值不足时结束探索；需要用户补充条件时填写 needs_input。"),
}


def tool_definitions():
    return [{"type": "function", "function": {"name": name, "description": description, "parameters": model.model_json_schema()}} for name, (model, description) in TOOLS.items()]
