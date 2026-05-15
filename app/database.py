from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from app.config import Settings


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS repositories (
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

CREATE TABLE IF NOT EXISTS author_aliases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository_id INTEGER NOT NULL,
    canonical_name TEXT NOT NULL,
    alias_name TEXT NOT NULL DEFAULT '',
    alias_email TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (repository_id) REFERENCES repositories(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_type TEXT NOT NULL,
    title TEXT NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    auto_generated INTEGER NOT NULL DEFAULT 0,
    llm_provider TEXT NOT NULL DEFAULT 'openai-compatible',
    llm_model TEXT NOT NULL DEFAULT '',
    prompt_version TEXT NOT NULL DEFAULT 'v1',
    source_snapshot TEXT NOT NULL,
    content TEXT NOT NULL,
    generated_content TEXT NOT NULL DEFAULT '',
    has_manual_edits INTEGER NOT NULL DEFAULT 0,
    generation_notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS commit_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repository TEXT NOT NULL,
    repository_path TEXT NOT NULL,
    commit_hash TEXT NOT NULL,
    author_name TEXT NOT NULL,
    author_email TEXT NOT NULL,
    authored_at TEXT NOT NULL,
    subject TEXT NOT NULL DEFAULT '',
    body TEXT NOT NULL DEFAULT '',
    changed_files TEXT NOT NULL DEFAULT '[]',
    diff_excerpt TEXT NOT NULL DEFAULT '',
    generated_summary TEXT NOT NULL DEFAULT '',
    llm_note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (repository_path, commit_hash)
);

CREATE TABLE IF NOT EXISTS schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_type TEXT NOT NULL UNIQUE,
    is_enabled INTEGER NOT NULL DEFAULT 0,
    cadence TEXT NOT NULL,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    weekday INTEGER,
    day_of_month INTEGER,
    last_run_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS job_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name TEXT NOT NULL,
    status TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class Database:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            conn.executescript(SCHEMA_SQL)
            self._migrate_author_aliases(conn)
            self._migrate_reports_content_model(conn)
            self._migrate_reports_uniqueness(conn)
            self._seed_defaults(conn)

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _seed_defaults(self, conn: sqlite3.Connection) -> None:
        default_schedules = [
            ("daily", 0, "daily", 18, 0, None, None),
            ("weekly", 0, "weekly", 18, 0, 4, None),
            ("monthly", 0, "monthly", 18, 0, None, 28),
            ("summary", 0, "monthly", 18, 15, None, 28),
        ]
        conn.executemany(
            """
            INSERT OR IGNORE INTO schedules (
                report_type, is_enabled, cadence, hour, minute, weekday, day_of_month
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            default_schedules,
        )
        defaults = {
            "llm_base_url": "",
            "llm_model": "",
            "llm_api_key": "",
            "timezone": "Asia/Shanghai",
            "llm_report_timeout_seconds": "45",
            "llm_commit_summary_timeout_seconds": "30",
        }
        conn.executemany(
            "INSERT OR IGNORE INTO app_settings (key, value) VALUES (?, ?)",
            defaults.items(),
        )

    def _migrate_author_aliases(self, conn: sqlite3.Connection) -> None:
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(author_aliases)").fetchall()
        }
        if "repository_id" in columns:
            return

        rows = [dict(row) for row in conn.execute("SELECT * FROM author_aliases").fetchall()]
        repositories = [dict(row) for row in conn.execute("SELECT id FROM repositories ORDER BY id ASC").fetchall()]

        conn.execute("ALTER TABLE author_aliases RENAME TO author_aliases_legacy")
        conn.execute(
            """
            CREATE TABLE author_aliases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repository_id INTEGER NOT NULL,
                canonical_name TEXT NOT NULL,
                alias_name TEXT NOT NULL DEFAULT '',
                alias_email TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (repository_id) REFERENCES repositories(id) ON DELETE CASCADE
            )
            """
        )

        if rows and repositories:
            payload = []
            for row in rows:
                for repository in repositories:
                    payload.append(
                        (
                            repository["id"],
                            row["canonical_name"],
                            row["alias_name"],
                            row["alias_email"],
                            row["created_at"],
                        )
                    )
            conn.executemany(
                """
                INSERT INTO author_aliases (
                    repository_id, canonical_name, alias_name, alias_email, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                payload,
            )

        conn.execute("DROP TABLE author_aliases_legacy")

    def _migrate_reports_uniqueness(self, conn: sqlite3.Connection) -> None:
        rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT *
                FROM reports
                ORDER BY report_type, period_start, period_end, datetime(created_at) DESC, id DESC
                """
            ).fetchall()
        ]
        grouped: dict[tuple[str, str, str], list[dict]] = {}
        for row in rows:
            key = (row["report_type"], row["period_start"], row["period_end"])
            grouped.setdefault(key, []).append(row)

        delete_ids: list[int] = []
        for duplicates in grouped.values():
            if len(duplicates) <= 1:
                continue
            edited_rows = [row for row in duplicates if self._row_has_manual_edits(row)]
            keeper = edited_rows[0] if edited_rows else duplicates[0]
            delete_ids.extend(row["id"] for row in duplicates if row["id"] != keeper["id"])

        if delete_ids:
            conn.executemany("DELETE FROM reports WHERE id = ?", [(item,) for item in delete_ids])

        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_reports_period_unique
            ON reports (report_type, period_start, period_end)
            """
        )

    def _migrate_reports_content_model(self, conn: sqlite3.Connection) -> None:
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(reports)").fetchall()
        }
        legacy_columns = [name for name in ("draft_content", "final_content", "status") if name in columns]
        if "content" not in columns:
            conn.execute("ALTER TABLE reports ADD COLUMN content TEXT NOT NULL DEFAULT ''")
        if "generated_content" not in columns:
            conn.execute("ALTER TABLE reports ADD COLUMN generated_content TEXT NOT NULL DEFAULT ''")
        if "has_manual_edits" not in columns:
            conn.execute("ALTER TABLE reports ADD COLUMN has_manual_edits INTEGER NOT NULL DEFAULT 0")

        select_columns = ["id", "content", "generated_content", "has_manual_edits", *legacy_columns]
        rows = [
            dict(row)
            for row in conn.execute(
                f"SELECT {', '.join(select_columns)} FROM reports"
            ).fetchall()
        ]
        for row in rows:
            generated_content = row.get("generated_content") or row.get("draft_content") or ""
            content = row.get("content") or row.get("final_content") or generated_content
            has_manual_edits = 1 if content != generated_content else 0
            if row.get("has_manual_edits") != has_manual_edits or row.get("content") != content or row.get("generated_content") != generated_content:
                conn.execute(
                    """
                    UPDATE reports
                    SET content = ?, generated_content = ?, has_manual_edits = ?, updated_at = updated_at
                    WHERE id = ?
                    """,
                    (content, generated_content, has_manual_edits, row["id"]),
                )

    @staticmethod
    def _row_has_manual_edits(row: dict) -> bool:
        if "has_manual_edits" in row:
            return bool(row["has_manual_edits"])
        final_content = row.get("final_content") or ""
        draft_content = row.get("draft_content") or ""
        return bool(final_content and final_content != draft_content)


def create_database(settings: Settings) -> Database:
    db = Database(settings.db_path)
    db.initialize()
    return db
