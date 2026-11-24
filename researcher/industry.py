import json
import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .config import ROOT

INDUSTRY = Path(os.environ.get("RESEARCHER_INDUSTRY_DIR", ROOT / "industry")).resolve()
Tier = Literal["T1", "T1.5", "T2"]


class Selection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    thresholds: dict[Tier, int]
    category_thresholds: dict[str, dict[Tier, int]]
    event_types: list[str]
    browse_floor: int = Field(ge=0, le=100)
    auto_research_score: int = Field(ge=0, le=100)
    auto_research_types: list[str]
    freshness_hours: int = Field(gt=0)
    backfill_months: int = Field(gt=0)
    backfill_limit: int = Field(gt=0, le=200)
    cluster_days: int = Field(gt=0)
    cluster_top: int = Field(gt=0, le=20)
    cluster_similarity: float = Field(ge=0, le=1)
    heat_half_life_hours: int = Field(gt=0)
    cadence_days: int = Field(gt=0)
    stale_multiplier: float = Field(gt=1)
    poll_bands: list[tuple[int, int]]
    onboarding_goal: str

    @model_validator(mode="after")
    def valid_policy(self):
        if set(self.thresholds) != {"T1", "T1.5", "T2"} or any(not 0 <= score <= 100 for group in (self.thresholds, *self.category_thresholds.values()) for score in group.values()):
            raise ValueError("分级评分门槛必须覆盖三个分级，且取值为 0 至 100")
        limits = [minimum for minimum, _ in self.poll_bands]
        if not limits or limits[-1] != 0 or limits != sorted(set(limits), reverse=True) or any(minutes <= 0 for _, minutes in self.poll_bands):
            raise ValueError("轮询档位按发文量从高到低排列，最后一档从 0 开始，轮询间隔必须为正")
        if not self.event_types or not set(self.auto_research_types) <= set(self.event_types):
            raise ValueError("自动核验类别必须来自已配置的事件类别")
        return self

    def threshold(self, tier, category):
        return self.category_thresholds.get(category, {}).get(tier, self.thresholds[tier])


def selection():
    return Selection.model_validate_json((INDUSTRY / "selection.json").read_text())


def source_catalog():
    return json.loads((INDUSTRY / "sources.json").read_text())


def prompt(step):
    return (INDUSTRY / "prompts" / f"{step}.md").read_text()
