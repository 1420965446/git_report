from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


ReportType = Literal["daily", "weekly", "monthly", "summary"]
CadenceType = Literal["daily", "weekly", "monthly"]


class RepositoryCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    path: str = Field(min_length=1)
    is_enabled: bool = True
    exclude_merge_commits: bool = True
    exclude_bots: bool = True
    notes: str = ""


class RepositoryValidationRequest(BaseModel):
    path: str = Field(min_length=1)
    limit: int = Field(default=5, ge=1, le=20)


class RepositoryRead(RepositoryCreate):
    id: int
    created_at: str
    updated_at: str


class AliasCreate(BaseModel):
    repository_id: int = Field(ge=1)
    canonical_name: str = Field(min_length=1, max_length=100)
    alias_name: str = ""
    alias_email: str = ""


class AliasRead(AliasCreate):
    id: int
    repository_name: str
    created_at: str


class AliasDiscoveryItem(BaseModel):
    repository_id: int
    repository_name: str
    canonical_name: str
    alias_name: str
    alias_email: str
    commit_count: int


class AliasDiscoveryResponse(BaseModel):
    items: list[AliasDiscoveryItem]


class ScheduleUpdate(BaseModel):
    report_type: ReportType
    is_enabled: bool
    cadence: CadenceType
    hour: int = Field(ge=0, le=23)
    minute: int = Field(ge=0, le=59)
    weekday: int | None = Field(default=None, ge=0, le=6)
    day_of_month: int | None = Field(default=None, ge=1, le=31)


class ScheduleRead(ScheduleUpdate):
    id: int
    last_run_at: str | None = None
    created_at: str
    updated_at: str


class LlmSettingsUpdate(BaseModel):
    llm_base_url: str = ""
    llm_model: str = ""
    llm_api_key: str = ""
    timezone: str = "Asia/Shanghai"
    llm_report_timeout_seconds: int = Field(default=45, ge=10, le=600)
    llm_commit_summary_timeout_seconds: int = Field(default=30, ge=10, le=600)


class LlmTestResult(BaseModel):
    ok: bool
    message: str
    provider_status: int | None = None


class ReportGenerateRequest(BaseModel):
    report_type: ReportType
    period_start: str | None = None
    period_end: str | None = None
    auto_generated: bool = False
    overwrite_final: bool = False


class ReportUpdate(BaseModel):
    content: str


class ReportRead(BaseModel):
    id: int
    report_type: ReportType
    title: str
    period_start: str
    period_end: str
    auto_generated: bool
    llm_provider: str
    llm_model: str
    prompt_version: str
    source_snapshot: dict[str, Any]
    content: str
    generated_content: str
    has_manual_edits: bool
    generation_notes: str
    created_at: str
    updated_at: str


class GitValidationResult(BaseModel):
    ok: bool
    message: str
    resolved_path: str | None = None
    repository_id: int | None = None
    preview_count: int = 0
    matched_count: int = 0
    preview_commits: list[dict[str, Any]] = []


class ReportGenerationStatus(BaseModel):
    is_running: bool
    report_type: ReportType | None = None
    stage: str = ""
    detail: str = ""
    started_at: str | None = None
    finished_at: str | None = None
    error: str = ""
    report_id: int | None = None


class CommitSummaryRead(BaseModel):
    id: int
    repository: str
    repository_path: str
    commit_hash: str
    author_name: str
    author_email: str
    authored_at: str
    subject: str
    body: str
    changed_files: list[str]
    diff_excerpt: str
    generated_summary: str
    llm_note: str
    created_at: str
    updated_at: str


class CommitSummaryGenerateRequest(BaseModel):
    date: str = Field(min_length=10, max_length=10)


class CommitSummaryGenerateResult(BaseModel):
    date: str
    total_commits: int
    generated_count: int
    reused_count: int
    errors: list[str] = []
    summaries: list[CommitSummaryRead]


class DashboardResponse(BaseModel):
    repositories: list[RepositoryRead]
    aliases: list[AliasRead]
    schedules: list[ScheduleRead]
    reports: list[ReportRead]
    llm_settings: LlmSettingsUpdate
    recent_jobs: list[dict[str, Any]]
