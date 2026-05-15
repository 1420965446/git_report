from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from app.database import Database
from app.git_utils import CommitRecord, GitRepositoryError, _decode_git_output, _parse_changed_files, author_matches, collect_commits
from app.llm import LlmClient, _append_diagnostic, _build_chat_completions_url, _extract_status_code
from app.reporting import (
    build_fallback_draft,
    build_prompt,
    build_structured_summary,
    compute_period,
    generate_commit_summaries_for_period,
    generate_report,
    list_commit_summaries,
    resolve_timezone,
)


class ReportingTestCase(unittest.TestCase):
    def test_daily_period_has_same_day_bounds(self) -> None:
        start_at, end_at = compute_period("daily", "Asia/Shanghai", "2026-05-13T00:00:00", "2026-05-13T23:59:59")
        self.assertEqual(start_at.date().isoformat(), "2026-05-13")
        self.assertEqual(end_at.date().isoformat(), "2026-05-13")

    def test_fallback_for_empty_commits(self) -> None:
        summary = build_structured_summary("daily", *compute_period("daily", "Asia/Shanghai", "2026-05-13T00:00:00", "2026-05-13T23:59:59"), [])
        draft = build_fallback_draft("daily", summary)
        self.assertIn("未识别到本人有效提交记录", draft)

    def test_structured_summary_uses_generated_commit_summaries(self) -> None:
        start_at, end_at = compute_period("daily", "Asia/Shanghai", "2026-05-13T00:00:00", "2026-05-13T23:59:59")
        commit_items = [
            {
                "repository": "demo",
                "commit_hash": "abc123",
                "authored_at": "2026-05-13T10:00:00+08:00",
                "subject": "fix",
                "changed_files": ["app/service.py"],
                "generated_summary": "完善 service 返回结果，并补充日报来源标识。",
            }
        ]
        summary = build_structured_summary("daily", start_at, end_at, commit_items)
        self.assertEqual(summary["period_label"], "2026-05-13 日报")
        self.assertEqual(summary["period_start_date"], "2026-05-13")
        self.assertEqual(summary["period_end_date"], "2026-05-13")
        self.assertEqual(summary["repositories"][0]["changed_files"], ["app/service.py"])
        self.assertIn("完善 service 返回结果，并补充日报来源标识。", summary["repositories"][0]["change_focus"])
        self.assertEqual(summary["commits"][0]["subject_hint"], "fix")
        self.assertIn("app/service.py", summary["commits"][0]["changed_files"])
        self.assertEqual(summary["commits"][0]["summary"], "完善 service 返回结果，并补充日报来源标识。")

    def test_build_prompt_includes_absolute_period_dates(self) -> None:
        start_at, end_at = compute_period("daily", "Asia/Shanghai", "2026-05-13T00:00:00", "2026-05-13T23:59:59")
        summary = build_structured_summary(
            "daily",
            start_at,
            end_at,
            [
                {
                    "repository": "demo",
                    "commit_hash": "abc123",
                    "authored_at": "2026-05-13T10:00:00+08:00",
                    "subject": "fix",
                    "changed_files": ["app/service.py"],
                    "generated_summary": "完善 service 返回结果，并补充日报来源标识。",
                }
            ],
        )
        prompt = build_prompt("daily", summary)
        self.assertIn("period_label", prompt)
        self.assertIn("period_start_date", prompt)
        self.assertIn("period_end_date", prompt)
        self.assertIn("2026-05-13", prompt)

    def test_daily_prompt_treats_explanations_as_optional(self) -> None:
        start_at, end_at = compute_period("daily", "Asia/Shanghai", "2026-05-13T00:00:00", "2026-05-13T23:59:59")
        summary = build_structured_summary(
            "daily",
            start_at,
            end_at,
            [
                {
                    "repository": "demo",
                    "commit_hash": "abc123",
                    "authored_at": "2026-05-13T10:00:00+08:00",
                    "subject": "fix",
                    "changed_files": ["app/service.py"],
                    "generated_summary": "完善 service 返回结果，并补充日报来源标识。",
                }
            ],
        )
        prompt = build_prompt("daily", summary)
        self.assertIn("聚焦今日完成、变更摘要和说明", prompt)
        self.assertIn("不要仅凭 git 记录编造“风险阻塞”", prompt)
        self.assertIn("才补充“说明”，否则省略该部分", prompt)

    def test_weekly_prompt_treats_explanations_as_optional(self) -> None:
        start_at, end_at = compute_period("weekly", "Asia/Shanghai", "2026-05-12T00:00:00", "2026-05-18T23:59:59")
        summary = {
            "report_type": "weekly",
            "period_label": "2026-05-12 至 2026-05-18 周报",
            "period_start": start_at.isoformat(),
            "period_end": end_at.isoformat(),
            "period_start_date": "2026-05-12",
            "period_end_date": "2026-05-18",
            "daily_report_count": 2,
            "empty_daily_count": 1,
            "daily_reports": [
                {
                    "date": "2026-05-12",
                    "report_id": 1,
                    "title": "2026-05-12 日报",
                    "status": "draft",
                    "content_source": "draft_content",
                    "is_empty": False,
                    "content": "完成日报生成功能修复。",
                },
                {
                    "date": "2026-05-13",
                    "report_id": 2,
                    "title": "2026-05-13 日报",
                    "status": "draft",
                    "content_source": "draft_content",
                    "is_empty": True,
                    "content": "当前周期未识别到有效提交，可补充非代码工作。",
                },
            ],
        }
        prompt = build_prompt("weekly", summary)
        self.assertIn("聚焦本周完成事项、重点进展和说明", prompt)
        self.assertIn("输入的每日日报信息需要尽可能详尽地吸收进周报", prompt)
        self.assertIn("不要把代码提交直接推断成“风险问题”", prompt)
        self.assertIn("才写“说明”，否则省略该部分", prompt)

    def test_parse_changed_files_from_diff(self) -> None:
        diff_text = "\n".join(
            [
                "diff --git a/app/main.py b/app/main.py",
                "diff --git a/tests/test_main.py b/tests/test_main.py",
            ]
        )
        self.assertEqual(_parse_changed_files(diff_text), ["app/main.py", "tests/test_main.py"])

    def test_collect_commits_keeps_entries_with_empty_body(self) -> None:
        original_validate = collect_commits.__globals__["validate_git_repository"]
        original_run = collect_commits.__globals__["_run_git_command"]
        try:
            collect_commits.__globals__["validate_git_repository"] = lambda path: (True, "ok")

            class Result:
                returncode = 0
                stderr = b""
                stdout = (
                    "hash1\x1fLenovo-pc-fhd\x1f1420965446@qq.com\x1f2026-05-12T16:58:31+08:00\x1frefactor\x1fbody\x1e\n"
                    "hash2\x1fLenovo-pc-fhd\x1f1420965446@qq.com\x1f2026-05-12T16:58:23+08:00\x1ffeat(column)\x1f\x1e\n"
                    "hash3\x1fLenovo-pc-fhd\x1f1420965446@qq.com\x1f2026-05-12T15:25:48+08:00\x1ffix(news)\x1f\x1e\n"
                ).encode("utf-8")

            collect_commits.__globals__["_run_git_command"] = lambda command: Result()
            records = collect_commits(
                repo_name="demo",
                repo_path="C:/repo/demo",
                since_iso="2026-05-12T00:00:00+08:00",
                until_iso="2026-05-12T23:59:59+08:00",
                exclude_merge_commits=True,
                include_diff=False,
            )
        finally:
            collect_commits.__globals__["validate_git_repository"] = original_validate
            collect_commits.__globals__["_run_git_command"] = original_run

        self.assertEqual(len(records), 3)
        self.assertEqual(records[1].commit_hash, "hash2")
        self.assertEqual(records[1].subject, "feat(column)")
        self.assertEqual(records[1].body, "")

    def test_database_initializes_default_schedules(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(Path(temp_dir) / "app.db")
            db.initialize()
            with db.connection() as conn:
                count = conn.execute("SELECT COUNT(*) AS count FROM schedules").fetchone()["count"]
                self.assertGreaterEqual(count, 4)

    def test_database_migrates_global_aliases_to_repository_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "app.db"
            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    CREATE TABLE repositories (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        path TEXT NOT NULL UNIQUE,
                        is_enabled INTEGER NOT NULL DEFAULT 1,
                        exclude_merge_commits INTEGER NOT NULL DEFAULT 1,
                        exclude_bots INTEGER NOT NULL DEFAULT 1,
                        notes TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE TABLE author_aliases (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        canonical_name TEXT NOT NULL,
                        alias_name TEXT NOT NULL DEFAULT '',
                        alias_email TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                    INSERT INTO repositories (name, path) VALUES ('repo-a', 'C:/repo-a');
                    INSERT INTO repositories (name, path) VALUES ('repo-b', 'C:/repo-b');
                    INSERT INTO author_aliases (canonical_name, alias_name, alias_email)
                    VALUES ('张三', 'zhangsan', 'me@company.com');
                    """
                )
                conn.commit()
            finally:
                conn.close()
            db = Database(db_path)
            db.initialize()
            with db.connection() as conn:
                columns = [row["name"] for row in conn.execute("PRAGMA table_info(author_aliases)").fetchall()]
                self.assertIn("repository_id", columns)
                rows = [dict(row) for row in conn.execute("SELECT repository_id, canonical_name FROM author_aliases ORDER BY repository_id").fetchall()]
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0]["canonical_name"], "张三")
                self.assertEqual(rows[1]["canonical_name"], "张三")

    def test_generate_report_emits_progress_updates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(Path(temp_dir) / "app.db")
            db.initialize()
            with db.connection() as conn:
                stages = []

                class StubLlmClient:
                    model = "stub-model"

                    def generate_commit_summary(self, commit_payload: dict) -> tuple[str | None, str]:
                        return f"已归纳提交：{commit_payload['subject']}", "提交摘要生成成功。"

                    def generate_report(self, prompt: str, metadata: dict | None = None) -> tuple[str | None, str]:
                        self.prompt = prompt
                        self.metadata = metadata
                        return "stub report", "LLM 生成成功。"

                report = generate_report(
                    conn=conn,
                    report_type="daily",
                    timezone="Asia/Shanghai",
                    llm_client=StubLlmClient(),
                    progress_callback=lambda stage, detail: stages.append((stage, detail)),
                )
                self.assertEqual(report["report_type"], "daily")
                self.assertEqual([item[0] for item in stages], ["prepare", "scan", "summarize", "llm", "save", "done"])
                self.assertEqual(report["prompt_version"], "v3-commit-summary-based")

    def test_daily_report_upserts_same_period(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(Path(temp_dir) / "app.db")
            db.initialize()
            with db.connection() as conn:
                class StubLlmClient:
                    model = "stub-model"

                    def generate_commit_summary(self, commit_payload: dict) -> tuple[str | None, str]:
                        return "commit summary", "提交摘要生成成功。"

                    def generate_report(self, prompt: str, metadata: dict | None = None) -> tuple[str | None, str]:
                        return "daily report", "LLM 生成成功。"

                report1 = generate_report(
                    conn=conn,
                    report_type="daily",
                    timezone="Asia/Shanghai",
                    llm_client=StubLlmClient(),
                    period_start="2026-05-13T00:00:00",
                    period_end="2026-05-13T23:59:59",
                )
                report2 = generate_report(
                    conn=conn,
                    report_type="daily",
                    timezone="Asia/Shanghai",
                    llm_client=StubLlmClient(),
                    period_start="2026-05-13T00:00:00",
                    period_end="2026-05-13T23:59:59",
                )
                count = conn.execute("SELECT COUNT(*) AS count FROM reports WHERE report_type = 'daily'").fetchone()["count"]
                self.assertEqual(count, 1)
                self.assertEqual(report1["id"], report2["id"])

    def test_weekly_report_backfills_missing_daily_reports(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(Path(temp_dir) / "app.db")
            db.initialize()
            with db.connection() as conn:
                class StubLlmClient:
                    model = "stub-model"

                    def generate_commit_summary(self, commit_payload: dict) -> tuple[str | None, str]:
                        return "commit summary", "提交摘要生成成功。"

                    def generate_report(self, prompt: str, metadata: dict | None = None) -> tuple[str | None, str]:
                        if "本周每日日报" in prompt:
                            return "weekly report", "LLM 生成成功。"
                        return "daily report", "LLM 生成成功。"

                report = generate_report(
                    conn=conn,
                    report_type="weekly",
                    timezone="Asia/Shanghai",
                    llm_client=StubLlmClient(),
                    period_start="2026-05-11T00:00:00",
                    period_end="2026-05-17T23:59:59",
                )
                report_counts = {
                    row["report_type"]: row["count"]
                    for row in conn.execute("SELECT report_type, COUNT(*) AS count FROM reports GROUP BY report_type").fetchall()
                }
                self.assertEqual(report_counts["daily"], 7)
                self.assertEqual(report_counts["weekly"], 1)
                self.assertEqual(report["prompt_version"], "v4-weekly-from-daily")
                self.assertEqual(report["source_snapshot"]["daily_report_count"], 7)
                self.assertIn("补生成日报 7 天", report["generation_notes"])

    def test_weekly_report_reuses_existing_daily_reports(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(Path(temp_dir) / "app.db")
            db.initialize()
            with db.connection() as conn:
                existing_days = [
                    ("2026-05-12T00:00:00+08:00", "2026-05-12T23:59:59.999999+08:00", "draft day 1", "", '{"commit_count": 0}'),
                    ("2026-05-13T00:00:00+08:00", "2026-05-13T23:59:59.999999+08:00", "draft day 2", "final day 2", '{"commit_count": 1}'),
                ]
                for index, item in enumerate(existing_days, start=1):
                    conn.execute(
                        """
                        INSERT INTO reports (
                            report_type, title, period_start, period_end, status, auto_generated,
                            llm_provider, llm_model, prompt_version, source_snapshot,
                            draft_content, final_content, generation_notes
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "daily",
                            f"day-{index}",
                            item[0],
                            item[1],
                            "final" if item[3] else "draft",
                            0,
                            "openai-compatible",
                            "stub-model",
                            "v3-commit-summary-based",
                            item[4],
                            item[2],
                            item[3],
                            "",
                        ),
                    )

                class StubLlmClient:
                    model = "stub-model"

                    def generate_commit_summary(self, commit_payload: dict) -> tuple[str | None, str]:
                        return "commit summary", "提交摘要生成成功。"

                    def generate_report(self, prompt: str, metadata: dict | None = None) -> tuple[str | None, str]:
                        if "本周每日日报" in prompt:
                            return "weekly report", "LLM 生成成功。"
                        return "daily report", "LLM 生成成功。"

                report = generate_report(
                    conn=conn,
                    report_type="weekly",
                    timezone="Asia/Shanghai",
                    llm_client=StubLlmClient(),
                    period_start="2026-05-12T00:00:00",
                    period_end="2026-05-18T23:59:59",
                )
                daily_count = conn.execute("SELECT COUNT(*) AS count FROM reports WHERE report_type = 'daily'").fetchone()["count"]
                self.assertEqual(daily_count, 7)
                self.assertIn("复用已有日报 2 天", report["generation_notes"])
                day2 = next(item for item in report["source_snapshot"]["daily_reports"] if item["date"] == "2026-05-13")
                self.assertEqual(day2["content_source"], "final_content")
                self.assertEqual(day2["content"], "final day 2")

    def test_generate_report_reuses_persisted_commit_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(Path(temp_dir) / "app.db")
            db.initialize()
            with db.connection() as conn:
                conn.execute(
                    """
                    INSERT INTO repositories (name, path, is_enabled, exclude_merge_commits, exclude_bots, notes)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("demo", "C:/repo/demo", 1, 1, 1, ""),
                )
                repository_id = conn.execute("SELECT id FROM repositories WHERE path = ?", ("C:/repo/demo",)).fetchone()["id"]
                conn.execute(
                    """
                    INSERT INTO author_aliases (repository_id, canonical_name, alias_name, alias_email)
                    VALUES (?, ?, ?, ?)
                    """,
                    (repository_id, "张三", "", "me@company.com"),
                )
                conn.execute(
                    """
                    INSERT INTO commit_summaries (
                        repository, repository_path, commit_hash, author_name, author_email,
                        authored_at, subject, body, changed_files, diff_excerpt, generated_summary, llm_note
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "demo",
                        "C:/repo/demo",
                        "abc123",
                        "张三",
                        "me@company.com",
                        "2026-05-13T10:00:00+08:00",
                        "fix",
                        "",
                        '["app/service.py"]',
                        "diff excerpt",
                        "完善 service 返回结果，并补充日报来源标识。",
                        "提交摘要生成成功。",
                    ),
                )

                original_collect_commits = generate_report.__globals__["collect_commits"]

                def stub_collect_commits(*args, **kwargs):
                    return [
                        CommitRecord(
                            repository="demo",
                            repository_path="C:/repo/demo",
                            commit_hash="abc123",
                            author_name="张三",
                            author_email="me@company.com",
                            authored_at="2026-05-13T10:00:00+08:00",
                            subject="fix",
                            body="",
                            changed_files=["app/service.py"],
                            diff_text="diff --git a/app/service.py b/app/service.py",
                        )
                    ]

                generate_report.__globals__["collect_commits"] = stub_collect_commits
                try:
                    class ReuseStubLlmClient:
                        model = "stub-model"

                        def generate_commit_summary(self, commit_payload: dict) -> tuple[str | None, str]:
                            raise AssertionError("should reuse persisted commit summary")

                        def generate_report(self, prompt: str, metadata: dict | None = None) -> tuple[str | None, str]:
                            return "stub report", "LLM 生成成功。"

                    report = generate_report(
                        conn=conn,
                        report_type="daily",
                        timezone="Asia/Shanghai",
                        llm_client=ReuseStubLlmClient(),
                        period_start="2026-05-13T00:00:00",
                        period_end="2026-05-13T23:59:59",
                    )
                finally:
                    generate_report.__globals__["collect_commits"] = original_collect_commits

                self.assertIn("完善 service 返回结果", report["source_snapshot"]["commits"][0]["summary"])

    def test_generate_commit_summaries_for_period_reuses_existing_items(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(Path(temp_dir) / "app.db")
            db.initialize()
            with db.connection() as conn:
                conn.execute(
                    """
                    INSERT INTO repositories (name, path, is_enabled, exclude_merge_commits, exclude_bots, notes)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("demo", "C:/repo/demo", 1, 1, 1, ""),
                )
                repository_id = conn.execute("SELECT id FROM repositories WHERE path = ?", ("C:/repo/demo",)).fetchone()["id"]
                conn.execute(
                    """
                    INSERT INTO author_aliases (repository_id, canonical_name, alias_name, alias_email)
                    VALUES (?, ?, ?, ?)
                    """,
                    (repository_id, "张三", "", "me@company.com"),
                )

                original_collect_commits = generate_report.__globals__["collect_commits"]

                def stub_collect_commits(*args, **kwargs):
                    return [
                        CommitRecord(
                            repository="demo",
                            repository_path="C:/repo/demo",
                            commit_hash="abc123",
                            author_name="张三",
                            author_email="me@company.com",
                            authored_at="2026-05-13T10:00:00+08:00",
                            subject="fix",
                            body="",
                            changed_files=["app/service.py"],
                            diff_text="diff --git a/app/service.py b/app/service.py",
                        )
                    ]

                generate_report.__globals__["collect_commits"] = stub_collect_commits
                try:
                    class StubLlmClient:
                        model = "stub-model"

                        def generate_commit_summary(self, commit_payload: dict) -> tuple[str | None, str]:
                            return "summary text", "提交摘要生成成功。"

                    result1 = generate_commit_summaries_for_period(
                        conn,
                        *compute_period("daily", "Asia/Shanghai", "2026-05-13T00:00:00", "2026-05-13T23:59:59"),
                        llm_client=StubLlmClient(),
                    )
                    result2 = generate_commit_summaries_for_period(
                        conn,
                        *compute_period("daily", "Asia/Shanghai", "2026-05-13T00:00:00", "2026-05-13T23:59:59"),
                        llm_client=StubLlmClient(),
                    )
                finally:
                    generate_report.__globals__["collect_commits"] = original_collect_commits

                self.assertEqual(result1["total_commits"], 1)
                self.assertEqual(result1["generated_count"], 1)
                self.assertEqual(result2["generated_count"], 0)
                self.assertEqual(result2["reused_count"], 1)
                self.assertEqual(result2["errors"], [])

    def test_generate_commit_summaries_for_period_surfaces_collection_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(Path(temp_dir) / "app.db")
            db.initialize()
            with db.connection() as conn:
                repository_id = conn.execute(
                    """
                    INSERT INTO repositories (name, path)
                    VALUES (?, ?)
                    """,
                    ("demo", "C:/repo/demo"),
                ).lastrowid
                conn.execute(
                    """
                    INSERT INTO author_aliases (repository_id, canonical_name, alias_name, alias_email)
                    VALUES (?, ?, ?, ?)
                    """,
                    (repository_id, "张三", "张三", "me@company.com"),
                )

                original_collect_commits = generate_report.__globals__["collect_commits"]

                def stub_collect_commits(*args, **kwargs):
                    raise GitRepositoryError("fatal: detected dubious ownership")

                generate_report.__globals__["collect_commits"] = stub_collect_commits
                try:
                    class StubLlmClient:
                        model = "stub-model"

                        def generate_commit_summary(self, commit_payload: dict) -> tuple[str | None, str]:
                            return "summary text", "提交摘要生成成功。"

                    result = generate_commit_summaries_for_period(
                        conn,
                        *compute_period("daily", "Asia/Shanghai", "2026-05-12T00:00:00", "2026-05-12T23:59:59"),
                        llm_client=StubLlmClient(),
                    )
                finally:
                    generate_report.__globals__["collect_commits"] = original_collect_commits

                self.assertEqual(result["total_commits"], 0)
                self.assertEqual(result["generated_count"], 0)
                self.assertEqual(result["reused_count"], 0)
                self.assertEqual(result["errors"], ["demo: fatal: detected dubious ownership"])

    def test_list_commit_summaries_filters_by_date(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db = Database(Path(temp_dir) / "app.db")
            db.initialize()
            with db.connection() as conn:
                conn.execute(
                    """
                    INSERT INTO commit_summaries (
                        repository, repository_path, commit_hash, author_name, author_email,
                        authored_at, subject, body, changed_files, diff_excerpt, generated_summary, llm_note
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "demo",
                        "C:/repo/demo",
                        "abc123",
                        "张三",
                        "me@company.com",
                        "2026-05-13T10:00:00+08:00",
                        "fix",
                        "",
                        '["app/service.py"]',
                        "diff excerpt",
                        "summary day 13",
                        "提交摘要生成成功。",
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO commit_summaries (
                        repository, repository_path, commit_hash, author_name, author_email,
                        authored_at, subject, body, changed_files, diff_excerpt, generated_summary, llm_note
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "demo",
                        "C:/repo/demo",
                        "def456",
                        "张三",
                        "me@company.com",
                        "2026-05-14T11:00:00+08:00",
                        "feat",
                        "",
                        '["app/other.py"]',
                        "diff excerpt",
                        "summary day 14",
                        "提交摘要生成成功。",
                    ),
                )
                day13 = list_commit_summaries(conn, authored_date="2026-05-13")
                self.assertEqual(len(day13), 1)
                self.assertEqual(day13[0]["generated_summary"], "summary day 13")

    def test_database_migrates_duplicate_reports_to_single_period_record(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "app.db"
            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    CREATE TABLE reports (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        report_type TEXT NOT NULL,
                        title TEXT NOT NULL,
                        period_start TEXT NOT NULL,
                        period_end TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'draft',
                        auto_generated INTEGER NOT NULL DEFAULT 0,
                        llm_provider TEXT NOT NULL DEFAULT 'openai-compatible',
                        llm_model TEXT NOT NULL DEFAULT '',
                        prompt_version TEXT NOT NULL DEFAULT 'v1',
                        source_snapshot TEXT NOT NULL,
                        draft_content TEXT NOT NULL,
                        final_content TEXT NOT NULL DEFAULT '',
                        generation_notes TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    );
                    INSERT INTO reports (
                        report_type, title, period_start, period_end, status, source_snapshot, draft_content, final_content
                    ) VALUES (
                        'daily', 'draft report', '2026-05-13T00:00:00+08:00', '2026-05-13T23:59:59.999999+08:00',
                        'draft', '{}', 'draft', ''
                    );
                    INSERT INTO reports (
                        report_type, title, period_start, period_end, status, source_snapshot, draft_content, final_content
                    ) VALUES (
                        'daily', 'final report', '2026-05-13T00:00:00+08:00', '2026-05-13T23:59:59.999999+08:00',
                        'final', '{}', 'draft newer', 'final keep'
                    );
                    """
                )
                conn.commit()
            finally:
                conn.close()

            db = Database(db_path)
            db.initialize()
            with db.connection() as conn2:
                rows = conn2.execute(
                    "SELECT id, title, status, final_content FROM reports WHERE report_type = 'daily'"
                ).fetchall()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["status"], "final")
                self.assertEqual(rows[0]["final_content"], "final keep")

    def test_llm_runtime_diagnostic_mentions_timeout_and_prompt_size(self) -> None:
        client = LlmClient("https://api.example.com/v1", "demo-model", "test-key", 90, 40)
        message = client._build_runtime_diagnostic(
            "日报生成",
            90,
            "hello world",
            {"commit_count": 3, "repository_count": 2},
        )
        self.assertIn("日报生成", message)
        self.assertIn("90 秒", message)
        self.assertIn("11 个字符", message)
        self.assertIn("提交数=3", message)

    def test_llm_client_without_config_returns_fallback_notice(self) -> None:
        client = LlmClient("", "", "")
        content, note = client.generate_report("hello")
        self.assertIsNone(content)
        self.assertIn("LLM 未配置", note)

    def test_author_alias_matching_by_email(self) -> None:
        matched = author_matches(
            "someone",
            "me@company.com",
            [{"canonical_name": "张三", "alias_name": "", "alias_email": "me@company.com"}],
            True,
        )
        self.assertTrue(matched)

    def test_timezone_resolve_has_shanghai_fallback(self) -> None:
        zone = resolve_timezone("Asia/Shanghai")
        self.assertIsNotNone(zone)

    def test_git_output_decode_falls_back_from_utf8_to_gb18030(self) -> None:
        text = "修复周报生成"
        decoded = _decode_git_output(text.encode("gb18030"))
        self.assertEqual(decoded, text)

    def test_llm_connection_test_without_config(self) -> None:
        client = LlmClient("", "", "")
        ok, message, provider_status = client.test_connection()
        self.assertFalse(ok)
        self.assertIn("LLM 未配置", message)
        self.assertIsNone(provider_status)

    def test_extract_status_code_from_message(self) -> None:
        status_code = _extract_status_code("LLM 请求失败: HTTP 403 error code: 1010")
        self.assertEqual(status_code, 403)

    def test_append_diagnostic_for_403(self) -> None:
        message = _append_diagnostic(
            "LLM 请求失败: HTTP 403 error code: 1010",
            "https://api.example.com/v1",
            "deepseek-v4-pro",
        )
        self.assertIn("请求被服务端拒绝", message)
        self.assertIn("Base URL=https://api.example.com/v1", message)

    def test_build_chat_completion_url_from_host_root(self) -> None:
        url = _build_chat_completions_url("https://api.example.com")
        self.assertEqual(url, "https://api.example.com/v1/chat/completions")

    def test_build_chat_completion_url_from_v1(self) -> None:
        url = _build_chat_completions_url("https://api.example.com/v1")
        self.assertEqual(url, "https://api.example.com/v1/chat/completions")

    def test_build_chat_completion_url_from_full_endpoint(self) -> None:
        url = _build_chat_completions_url("https://api.example.com/v1/chat/completions")
        self.assertEqual(url, "https://api.example.com/v1/chat/completions")


if __name__ == "__main__":
    unittest.main()
