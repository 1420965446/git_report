from __future__ import annotations

import asyncio
from datetime import datetime

from app.llm import LlmClient
from app.reporting import generate_report, resolve_timezone


class ReportScheduler:
    def __init__(self, db, settings) -> None:
        self.db = db
        self.settings = settings
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        if self._task:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _loop(self) -> None:
        while self._running:
            try:
                self.run_pending()
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(60)

    def run_pending(self) -> None:
        with self.db.connection() as conn:
            timezone = self._get_setting(conn, "timezone", self.settings.timezone)
            now = datetime.now(resolve_timezone(timezone))
            schedules = conn.execute(
                "SELECT * FROM schedules WHERE is_enabled = 1 ORDER BY id ASC"
            ).fetchall()
            for schedule in schedules:
                if self._should_run(dict(schedule), now):
                    self._execute_schedule(conn, dict(schedule), timezone)

    def _execute_schedule(self, conn, schedule: dict, timezone: str) -> None:
        llm_client = LlmClient(
            self._get_setting(conn, "llm_base_url", self.settings.llm_base_url),
            self._get_setting(conn, "llm_model", self.settings.llm_model),
            self._get_setting(conn, "llm_api_key", self.settings.llm_api_key),
            self._get_setting(conn, "llm_report_timeout_seconds", self.settings.llm_report_timeout_seconds),
            self._get_setting(
                conn,
                "llm_commit_summary_timeout_seconds",
                self.settings.llm_commit_summary_timeout_seconds,
            ),
        )
        try:
            generate_report(
                conn=conn,
                report_type=schedule["report_type"],
                timezone=timezone,
                llm_client=llm_client,
                auto_generated=True,
            )
            conn.execute(
                "UPDATE schedules SET last_run_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (schedule["id"],),
            )
            conn.execute(
                "INSERT INTO job_runs (job_name, status, message) VALUES (?, ?, ?)",
                (f"{schedule['report_type']}-schedule", "success", "报告已自动生成"),
            )
        except Exception as exc:  # noqa: BLE001
            conn.execute(
                "INSERT INTO job_runs (job_name, status, message) VALUES (?, ?, ?)",
                (f"{schedule['report_type']}-schedule", "error", str(exc)),
            )

    def _get_setting(self, conn, key: str, fallback: str) -> str:
        row = conn.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else fallback

    def _should_run(self, schedule: dict, now: datetime) -> bool:
        if now.hour != schedule["hour"] or now.minute != schedule["minute"]:
            return False

        cadence = schedule["cadence"]
        if cadence == "daily":
            return self._not_ran_today(schedule, now)
        if cadence == "weekly":
            return schedule["weekday"] == now.weekday() and self._not_ran_today(schedule, now)
        if cadence == "monthly":
            day = schedule["day_of_month"] or 28
            return day == now.day and self._not_ran_today(schedule, now)
        return False

    @staticmethod
    def _not_ran_today(schedule: dict, now: datetime) -> bool:
        last_run_at = schedule.get("last_run_at")
        if not last_run_at:
            return True
        return last_run_at[:10] != now.date().isoformat()
