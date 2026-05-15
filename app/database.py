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
                SELECT id, report_type, period_start, period_end, status, created_at
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
            final_rows = [row for row in duplicates if row["status"] == "final"]
            keeper = final_rows[0] if final_rows else duplicates[0]
            delete_ids.extend(row["id"] for row in duplicates if row["id"] != keeper["id"])

        if delete_ids:
            conn.executemany("DELETE FROM reports WHERE id = ?", [(item,) for item in delete_ids])

        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_reports_period_unique
            ON reports (report_type, period_start, period_end)
            """
        )


def create_database(settings: Settings) -> Database:
    db = Database(settings.db_path)
    db.initialize()
    return db
