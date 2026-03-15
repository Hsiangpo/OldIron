"""England 来源提交策略。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from england_crawler.cluster.repository import CH_PIPELINE
from england_crawler.cluster.repository import DNB_PIPELINE
from england_crawler.cluster.repository import ClusterRepository


@dataclass(slots=True)
class EnglandSourceOutcome:
    """England 来源提交结果。"""

    source_key: str
    label: str
    state: str
    action: str
    task_count: int = 0

    def render(self) -> str:
        if self.action == "submitted":
            return f"{self.label} | 新增任务 {self.task_count}"
        if self.action == "requeued_failed":
            return f"{self.label} | 已重挂失败任务 {self.task_count}"
        if self.action == "in_progress":
            return f"{self.label} | 进行中，未重复补种"
        return f"{self.label} | 已完成，跳过"


@dataclass(slots=True)
class _EnglandSourceSpec:
    source_key: str
    label: str

    def detect_state(self, repo: ClusterRepository, *, input_xlsx: Path, max_companies: int) -> str:
        if self.source_key == "dnb":
            return repo.get_dnb_source_state()
        return repo.get_companies_house_source_state(
            input_xlsx,
            max_companies=max_companies,
        )

    def submit_missing(self, repo: ClusterRepository, *, input_xlsx: Path, max_companies: int) -> int:
        if self.source_key == "dnb":
            return repo.submit_dnb_seed_tasks()
        return repo.submit_companies_house_input(
            input_xlsx,
            max_companies=max_companies,
        )

    def requeue_failed(self, repo: ClusterRepository) -> int:
        pipeline = DNB_PIPELINE if self.source_key == "dnb" else CH_PIPELINE
        return repo.requeue_failed_tasks_for_pipeline(pipeline)


def submit_england_sources(
    repo: ClusterRepository,
    *,
    input_xlsx: Path,
    max_companies: int = 0,
) -> list[EnglandSourceOutcome]:
    """提交 England 下全部已注册来源。"""

    specs = [
        _EnglandSourceSpec(source_key="dnb", label="DNB"),
        _EnglandSourceSpec(source_key="companies-house", label="Companies House"),
    ]
    outcomes: list[EnglandSourceOutcome] = []
    source_path = Path(input_xlsx).resolve()
    for spec in specs:
        state = spec.detect_state(
            repo,
            input_xlsx=source_path,
            max_companies=max(int(max_companies), 0),
        )
        if state == "uninitialized":
            outcomes.append(
                EnglandSourceOutcome(
                    source_key=spec.source_key,
                    label=spec.label,
                    state=state,
                    action="submitted",
                    task_count=spec.submit_missing(
                        repo,
                        input_xlsx=source_path,
                        max_companies=max_companies,
                    ),
                )
            )
            continue
        if state == "failed_only":
            outcomes.append(
                EnglandSourceOutcome(
                    source_key=spec.source_key,
                    label=spec.label,
                    state=state,
                    action="requeued_failed",
                    task_count=spec.requeue_failed(repo),
                )
            )
            continue
        if state == "in_progress":
            outcomes.append(
                EnglandSourceOutcome(
                    source_key=spec.source_key,
                    label=spec.label,
                    state=state,
                    action="in_progress",
                )
            )
            continue
        outcomes.append(
            EnglandSourceOutcome(
                source_key=spec.source_key,
                label=spec.label,
                state=state,
                action="done",
            )
        )
    return outcomes
