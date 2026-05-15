from __future__ import annotations

import json
from datetime import datetime
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Lock

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from app.config import get_settings
from app.database import create_database
from app.git_utils import (
    GitRepositoryError,
    author_matches,
    collect_recent_commits,
    resolve_repository_path,
    validate_git_repository,
)
from app.llm import LlmClient
from app.reporting import (
    compute_period,
    generate_commit_summaries_for_period,
    generate_report,
    get_report,
    list_commit_summaries,
    list_reports,
)
from app.scheduler import ReportScheduler
from app.schemas import (
    AliasCreate,
    AliasDiscoveryResponse,
    CommitSummaryGenerateRequest,
    CommitSummaryGenerateResult,
    CommitSummaryRead,
    DashboardResponse,
    GitValidationResult,
    LlmSettingsUpdate,
    LlmTestResult,
    ReportGenerationStatus,
    ReportGenerateRequest,
    ReportRead,
    ReportUpdate,
    RepositoryCreate,
    RepositoryValidationRequest,
    ScheduleRead,
    ScheduleUpdate,
)

settings = get_settings()
db = create_database(settings)
scheduler = ReportScheduler(db, settings)
generation_status_lock = Lock()
generation_execution_lock = Lock()
generation_status: dict = {
    "is_running": False,
    "report_type": None,
    "stage": "",
    "detail": "",
    "started_at": None,
    "finished_at": None,
    "error": "",
    "report_id": None,
}


@asynccontextmanager
async def lifespan(_: FastAPI):
    await scheduler.start()
    yield
    await scheduler.stop()


app = FastAPI(title="Git Work Report Automation", lifespan=lifespan)


def _llm_client(conn) -> LlmClient:
    values = {
        row["key"]: row["value"]
        for row in conn.execute("SELECT key, value FROM app_settings").fetchall()
    }
    return LlmClient(
        values.get("llm_base_url", settings.llm_base_url),
        values.get("llm_model", settings.llm_model),
        values.get("llm_api_key", settings.llm_api_key),
        values.get("llm_report_timeout_seconds", settings.llm_report_timeout_seconds),
        values.get("llm_commit_summary_timeout_seconds", settings.llm_commit_summary_timeout_seconds),
    )


@app.get("/api/system/select-directory")
def select_directory() -> dict:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"当前环境不支持打开文件夹选择器：{exc}") from exc

    root = None
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        selected_path = filedialog.askdirectory(title="选择 Git 仓库文件夹")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"打开文件夹选择器失败：{exc}") from exc
    finally:
        if root is not None:
            root.destroy()

    return {
        "ok": bool(selected_path),
        "path": selected_path or "",
    }


def _set_generation_status(**updates) -> dict:
    with generation_status_lock:
        generation_status.update(updates)
        return dict(generation_status)


def _get_generation_status() -> dict:
    with generation_status_lock:
        return dict(generation_status)


def _dashboard_payload() -> dict:
    with db.connection() as conn:
        repositories = [dict(row) for row in conn.execute("SELECT * FROM repositories ORDER BY id DESC")]
        aliases = [
            dict(row)
            for row in conn.execute(
                """
                SELECT author_aliases.*, repositories.name AS repository_name
                FROM author_aliases
                JOIN repositories ON repositories.id = author_aliases.repository_id
                ORDER BY author_aliases.id DESC
                """
            )
        ]
        schedules = [dict(row) for row in conn.execute("SELECT * FROM schedules ORDER BY id ASC")]
        llm_settings = {
            row["key"]: row["value"]
            for row in conn.execute("SELECT key, value FROM app_settings").fetchall()
        }
        jobs = [dict(row) for row in conn.execute("SELECT * FROM job_runs ORDER BY id DESC LIMIT 20")]
        for report in repositories:
            report["is_enabled"] = bool(report["is_enabled"])
            report["exclude_merge_commits"] = bool(report["exclude_merge_commits"])
            report["exclude_bots"] = bool(report["exclude_bots"])
        for item in schedules:
            item["is_enabled"] = bool(item["is_enabled"])
            return {
                "repositories": repositories,
                "aliases": aliases,
                "schedules": schedules,
                "reports": list_reports(conn, 30),
                "llm_settings": {
                    "llm_base_url": llm_settings.get("llm_base_url", ""),
                    "llm_model": llm_settings.get("llm_model", ""),
                    "llm_api_key": llm_settings.get("llm_api_key", ""),
                    "timezone": llm_settings.get("timezone", settings.timezone),
                    "llm_report_timeout_seconds": int(llm_settings.get("llm_report_timeout_seconds", settings.llm_report_timeout_seconds)),
                    "llm_commit_summary_timeout_seconds": int(
                        llm_settings.get(
                            "llm_commit_summary_timeout_seconds",
                            settings.llm_commit_summary_timeout_seconds,
                        )
                    ),
                },
                "recent_jobs": jobs,
            }


@app.get("/", response_class=FileResponse)
def index() -> FileResponse:
    return FileResponse(Path(__file__).with_name("templates") / "index.html")


@app.get("/api/dashboard", response_model=DashboardResponse)
def dashboard() -> dict:
    return _dashboard_payload()


@app.post("/api/repositories")
def create_repository(payload: RepositoryCreate) -> dict:
    ok, message = validate_git_repository(payload.path)
    if not ok:
        raise HTTPException(status_code=400, detail=message)
    with db.connection() as conn:
        try:
            conn.execute(
                """
                INSERT INTO repositories (
                    name, path, is_enabled, exclude_merge_commits, exclude_bots, notes, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    payload.name,
                    resolve_repository_path(payload.path),
                    int(payload.is_enabled),
                    int(payload.exclude_merge_commits),
                    int(payload.exclude_bots),
                    payload.notes,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True}


@app.put("/api/repositories/{repository_id}")
def update_repository(repository_id: int, payload: RepositoryCreate) -> dict:
    ok, message = validate_git_repository(payload.path)
    if not ok:
        raise HTTPException(status_code=400, detail=message)
    with db.connection() as conn:
        existing = conn.execute(
            "SELECT id FROM repositories WHERE id = ?",
            (repository_id,),
        ).fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="仓库不存在")
        try:
            conn.execute(
                """
                UPDATE repositories
                SET name = ?, path = ?, is_enabled = ?, exclude_merge_commits = ?,
                    exclude_bots = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    payload.name,
                    resolve_repository_path(payload.path),
                    int(payload.is_enabled),
                    int(payload.exclude_merge_commits),
                    int(payload.exclude_bots),
                    payload.notes,
                    repository_id,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True}


@app.delete("/api/repositories/{repository_id}")
def delete_repository(repository_id: int) -> dict:
    with db.connection() as conn:
        conn.execute("DELETE FROM repositories WHERE id = ?", (repository_id,))
    return {"ok": True}


@app.post("/api/aliases")
def create_alias(payload: AliasCreate) -> dict:
    if not payload.alias_name and not payload.alias_email:
        raise HTTPException(status_code=400, detail="alias_name 和 alias_email 至少填写一个")
    with db.connection() as conn:
        repository = conn.execute(
            "SELECT id FROM repositories WHERE id = ?",
            (payload.repository_id,),
        ).fetchone()
        if not repository:
            raise HTTPException(status_code=400, detail="repository_id 对应的仓库不存在")
        conn.execute(
            """
            INSERT INTO author_aliases (repository_id, canonical_name, alias_name, alias_email)
            VALUES (?, ?, ?, ?)
            """,
            (payload.repository_id, payload.canonical_name, payload.alias_name, payload.alias_email),
        )
    return {"ok": True}


@app.delete("/api/aliases/{alias_id}")
def delete_alias(alias_id: int) -> dict:
    with db.connection() as conn:
        conn.execute("DELETE FROM author_aliases WHERE id = ?", (alias_id,))
    return {"ok": True}


@app.get("/api/aliases/discover", response_model=AliasDiscoveryResponse)
def discover_aliases() -> dict:
    with db.connection() as conn:
        repositories = [dict(row) for row in conn.execute("SELECT * FROM repositories WHERE is_enabled = 1 ORDER BY id DESC")]
        aliases = [dict(row) for row in conn.execute("SELECT * FROM author_aliases ORDER BY id DESC")]

    aliases_by_repo: dict[int, list[dict]] = {}
    for alias in aliases:
        aliases_by_repo.setdefault(int(alias["repository_id"]), []).append(alias)

    discovered: dict[tuple[int, str, str], dict] = {}
    for repository in repositories:
        repo_aliases = aliases_by_repo.get(int(repository["id"]), [])
        known_names = {
            value.strip().lower()
            for alias in repo_aliases
            for value in (alias["canonical_name"], alias["alias_name"])
            if value.strip()
        }
        known_emails = {
            alias["alias_email"].strip().lower()
            for alias in repo_aliases
            if alias["alias_email"].strip()
        }
        try:
            commits = collect_recent_commits(
                repo_name=repository["name"],
                repo_path=repository["path"],
                limit=50,
                exclude_merge_commits=bool(repository["exclude_merge_commits"]),
            )
        except GitRepositoryError:
            continue

        for commit in commits:
            normalized_name = commit.author_name.strip().lower()
            normalized_email = commit.author_email.strip().lower()
            if author_matches(commit.author_name, commit.author_email, repo_aliases, bool(repository["exclude_bots"])):
                continue
            if normalized_name in known_names or normalized_email in known_emails:
                continue
            if bool(repository["exclude_bots"]) and ("[bot]" in normalized_name or "bot@" in normalized_email or "noreply" in normalized_email):
                continue

            key = (int(repository["id"]), commit.author_name.strip(), commit.author_email.strip())
            item = discovered.setdefault(
                key,
                {
                    "repository_id": int(repository["id"]),
                    "repository_name": repository["name"],
                    "canonical_name": commit.author_name.strip() or commit.author_email.strip(),
                    "alias_name": commit.author_name.strip(),
                    "alias_email": commit.author_email.strip(),
                    "commit_count": 0,
                },
            )
            item["commit_count"] += 1

    items = []
    for value in sorted(
        discovered.values(),
        key=lambda item: (item["repository_name"].lower(), -item["commit_count"], item["canonical_name"].lower()),
    ):
        items.append(
            {
                "repository_id": value["repository_id"],
                "repository_name": value["repository_name"],
                "canonical_name": value["canonical_name"],
                "alias_name": value["alias_name"],
                "alias_email": value["alias_email"],
                "commit_count": value["commit_count"],
            }
        )
    return {"items": items}


@app.post("/api/repositories/validate", response_model=GitValidationResult)
def validate_repository(payload: RepositoryValidationRequest) -> dict:
    ok, message = validate_git_repository(payload.path)
    resolved_path = None
    preview_commits: list[dict] = []
    matched_count = 0
    repository_id = None
    try:
        resolved_path = resolve_repository_path(payload.path)
    except Exception:  # noqa: BLE001
        resolved_path = None
    if ok and resolved_path:
        with db.connection() as conn:
            repository = conn.execute(
                "SELECT id FROM repositories WHERE path = ?",
                (resolved_path,),
            ).fetchone()
            if repository:
                repository_id = int(repository["id"])
                aliases = [
                    dict(row)
                    for row in conn.execute(
                        "SELECT * FROM author_aliases WHERE repository_id = ? ORDER BY id DESC",
                        (repository_id,),
                    ).fetchall()
                ]
            else:
                aliases = []
        try:
            commits = collect_recent_commits(
                repo_name=Path(resolved_path).name or "repository",
                repo_path=resolved_path,
                limit=payload.limit,
            )
            for commit in commits:
                matched = author_matches(commit.author_name, commit.author_email, aliases, True)
                if matched:
                    matched_count += 1
                preview_commits.append(
                    {
                        "commit_hash": commit.commit_hash[:8],
                        "author_name": commit.author_name,
                        "author_email": commit.author_email,
                        "authored_at": commit.authored_at,
                        "subject": commit.subject,
                        "matched": matched,
                    }
                )
        except GitRepositoryError as exc:
            ok = False
            message = str(exc)
    return {
        "ok": ok,
        "message": message,
        "resolved_path": resolved_path,
        "repository_id": repository_id,
        "preview_count": len(preview_commits),
        "matched_count": matched_count,
        "preview_commits": preview_commits,
    }


@app.post("/api/reports/generate", response_model=ReportRead)
def api_generate_report(payload: ReportGenerateRequest) -> dict:
    with generation_execution_lock:
        current_status = _get_generation_status()
        if current_status["is_running"]:
            raise HTTPException(status_code=409, detail="当前已有报告正在生成，请等待本次任务完成。")

        _set_generation_status(
            is_running=True,
            report_type=payload.report_type,
            stage="prepare",
            detail="正在准备生成任务。",
            started_at=datetime.now().isoformat(),
            finished_at=None,
            error="",
            report_id=None,
        )
        with db.connection() as conn:
            timezone_row = conn.execute("SELECT value FROM app_settings WHERE key = 'timezone'").fetchone()
            timezone = timezone_row["value"] if timezone_row else settings.timezone
            try:
                report = generate_report(
                    conn=conn,
                    report_type=payload.report_type,
                    timezone=timezone,
                    llm_client=_llm_client(conn),
                    period_start=payload.period_start,
                    period_end=payload.period_end,
                    auto_generated=payload.auto_generated,
                    overwrite_final=payload.overwrite_final,
                    progress_callback=lambda stage, detail: _set_generation_status(
                        stage=stage,
                        detail=detail,
                    ),
                )
                _set_generation_status(
                    is_running=False,
                    stage="done",
                    detail="报告已生成完成。",
                    finished_at=datetime.now().isoformat(),
                    report_id=report["id"],
                )
                return report
            except GitRepositoryError as exc:
                _set_generation_status(
                    is_running=False,
                    stage="error",
                    detail="生成失败。",
                    finished_at=datetime.now().isoformat(),
                    error=str(exc),
                )
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except ValueError as exc:
                _set_generation_status(
                    is_running=False,
                    stage="error",
                    detail="生成失败。",
                    finished_at=datetime.now().isoformat(),
                    error=str(exc),
                )
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except Exception as exc:  # noqa: BLE001
                _set_generation_status(
                    is_running=False,
                    stage="error",
                    detail="生成失败。",
                    finished_at=datetime.now().isoformat(),
                    error=str(exc),
                )
                raise


@app.get("/api/reports/generation-status", response_model=ReportGenerationStatus)
def api_report_generation_status() -> dict:
    return _get_generation_status()


@app.get("/api/reports", response_model=list[ReportRead])
def api_list_reports() -> list[dict]:
    with db.connection() as conn:
        return list_reports(conn, 100)


@app.get("/api/commit-summaries", response_model=list[CommitSummaryRead])
def api_list_commit_summaries(date: str | None = None) -> list[dict]:
    with db.connection() as conn:
        return list_commit_summaries(conn, authored_date=date, limit=300)


@app.post("/api/commit-summaries/generate", response_model=CommitSummaryGenerateResult)
def api_generate_commit_summaries(payload: CommitSummaryGenerateRequest) -> dict:
    with generation_execution_lock:
        with db.connection() as conn:
            timezone_row = conn.execute("SELECT value FROM app_settings WHERE key = 'timezone'").fetchone()
            timezone = timezone_row["value"] if timezone_row else settings.timezone
            start_at, end_at = compute_period(
                "daily",
                timezone,
                f"{payload.date}T00:00:00",
                f"{payload.date}T23:59:59",
            )
            return generate_commit_summaries_for_period(
                conn=conn,
                start_at=start_at,
                end_at=end_at,
                llm_client=_llm_client(conn),
            )


@app.get("/api/reports/{report_id}", response_model=ReportRead)
def api_get_report(report_id: int) -> dict:
    with db.connection() as conn:
        try:
            return get_report(conn, report_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="报告不存在") from exc


@app.put("/api/reports/{report_id}", response_model=ReportRead)
def api_update_report(report_id: int, payload: ReportUpdate) -> dict:
    with db.connection() as conn:
        conn.execute(
            """
            UPDATE reports
            SET content = ?,
                has_manual_edits = CASE
                    WHEN ? = generated_content THEN 0
                    ELSE 1
                END,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (payload.content, payload.content, report_id),
        )
        return get_report(conn, report_id)


@app.delete("/api/reports/{report_id}")
def api_delete_report(report_id: int) -> dict:
    with db.connection() as conn:
        row = conn.execute("SELECT id FROM reports WHERE id = ?", (report_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="报告不存在")
        conn.execute("DELETE FROM reports WHERE id = ?", (report_id,))
        return {"ok": True}


@app.delete("/api/reports/{report_id}/with-summaries")
def api_delete_report_with_summaries(report_id: int) -> dict:
    with db.connection() as conn:
        row = conn.execute(
            "SELECT id, report_type, period_start, period_end FROM reports WHERE id = ?",
            (report_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="报告不存在")

        deleted_summary_count = 0
        if row["report_type"] == "daily":
            deleted_summary_count = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM commit_summaries
                WHERE authored_at >= ? AND authored_at <= ?
                """,
                (row["period_start"], row["period_end"]),
            ).fetchone()["count"]
            conn.execute(
                """
                DELETE FROM commit_summaries
                WHERE authored_at >= ? AND authored_at <= ?
                """,
                (row["period_start"], row["period_end"]),
            )

        conn.execute("DELETE FROM reports WHERE id = ?", (report_id,))
        return {"ok": True, "deleted_summary_count": deleted_summary_count}


@app.delete("/api/reports")
def api_clear_reports() -> dict:
    with db.connection() as conn:
        deleted_count = conn.execute("SELECT COUNT(*) AS count FROM reports").fetchone()["count"]
        conn.execute("DELETE FROM reports")
        return {"ok": True, "deleted_count": deleted_count}


@app.get("/api/schedules", response_model=list[ScheduleRead])
def api_list_schedules() -> list[dict]:
    with db.connection() as conn:
        rows = conn.execute("SELECT * FROM schedules ORDER BY id ASC").fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["is_enabled"] = bool(item["is_enabled"])
            items.append(item)
        return items


@app.put("/api/schedules/{schedule_id}", response_model=ScheduleRead)
def api_update_schedule(schedule_id: int, payload: ScheduleUpdate) -> dict:
    with db.connection() as conn:
        conn.execute(
            """
            UPDATE schedules
            SET report_type = ?, is_enabled = ?, cadence = ?, hour = ?, minute = ?,
                weekday = ?, day_of_month = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                payload.report_type,
                int(payload.is_enabled),
                payload.cadence,
                payload.hour,
                payload.minute,
                payload.weekday,
                payload.day_of_month,
                schedule_id,
            ),
        )
        row = conn.execute("SELECT * FROM schedules WHERE id = ?", (schedule_id,)).fetchone()
        item = dict(row)
        item["is_enabled"] = bool(item["is_enabled"])
        return item


@app.get("/api/settings/llm", response_model=LlmSettingsUpdate)
def api_get_llm_settings() -> dict:
    return _dashboard_payload()["llm_settings"]


@app.put("/api/settings/llm", response_model=LlmSettingsUpdate)
def api_update_llm_settings(payload: LlmSettingsUpdate) -> dict:
    with db.connection() as conn:
        for key, value in payload.model_dump().items():
            conn.execute(
                """
                INSERT INTO app_settings (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
                """,
                (key, value),
            )
    return payload.model_dump()


@app.post("/api/settings/llm/test", response_model=LlmTestResult)
def api_test_llm_settings(payload: LlmSettingsUpdate) -> dict:
    client = LlmClient(
        payload.llm_base_url,
        payload.llm_model,
        payload.llm_api_key,
        payload.llm_report_timeout_seconds,
        payload.llm_commit_summary_timeout_seconds,
    )
    ok, message, provider_status = client.test_connection()
    return {
        "ok": ok,
        "message": message,
        "provider_status": provider_status,
    }


@app.post("/api/scheduler/run")
def api_run_scheduler() -> dict:
    scheduler.run_pending()
    return {"ok": True}


@app.get("/api/export")
def api_export_dashboard() -> dict:
    return json.loads(json.dumps(_dashboard_payload(), ensure_ascii=False))
