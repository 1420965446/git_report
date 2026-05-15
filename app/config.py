from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class Settings:
    db_path: Path
    llm_base_url: str
    llm_model: str
    llm_api_key: str
    timezone: str
    llm_report_timeout_seconds: int
    llm_commit_summary_timeout_seconds: int


def get_settings() -> Settings:
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)
    return Settings(
        db_path=Path(os.getenv("REPORTER_DB_PATH", data_dir / "app.db")),
        llm_base_url=os.getenv("REPORTER_LLM_BASE_URL", "").rstrip("/"),
        llm_model=os.getenv("REPORTER_LLM_MODEL", ""),
        llm_api_key=os.getenv("REPORTER_LLM_API_KEY", ""),
        timezone=os.getenv("REPORTER_TIMEZONE", "Asia/Shanghai"),
        llm_report_timeout_seconds=max(10, int(os.getenv("REPORTER_LLM_REPORT_TIMEOUT_SECONDS", "45"))),
        llm_commit_summary_timeout_seconds=max(10, int(os.getenv("REPORTER_LLM_COMMIT_SUMMARY_TIMEOUT_SECONDS", "30"))),
    )
