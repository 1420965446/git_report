from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import UTC, date, datetime, time, timedelta, timezone
from typing import Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.git_utils import CommitRecord, GitRepositoryError, author_matches, collect_commits, serialize_snapshot
from app.llm import LlmClient

MAX_CHANGE_LINES_PER_COMMIT = 24
MAX_CHANGE_LINE_LENGTH = 180
MAX_REPOSITORY_FILES = 20
MAX_COMMIT_SUMMARY_DIFF_CHARS = 2000


def resolve_timezone(timezone_name: str):
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        if timezone_name == "Asia/Shanghai":
            return timezone(timedelta(hours=8), name="Asia/Shanghai")
        return UTC


def compute_period(report_type: str, timezone: str, period_start: str | None, period_end: str | None) -> tuple[datetime, datetime]:
    zone = resolve_timezone(timezone)
    now = datetime.now(zone)

    if period_start and period_end:
        start = datetime.fromisoformat(period_start)
        end = datetime.fromisoformat(period_end)
        if start.tzinfo is None:
            start = start.replace(tzinfo=zone)
        else:
            start = start.astimezone(zone)
        if end.tzinfo is None:
            end = end.replace(tzinfo=zone)
        else:
            end = end.astimezone(zone)
        return start, end

    today = now.date()
    if report_type == "daily":
        target_day = today
        start_day = datetime.combine(target_day, time.min, zone)
        end_day = datetime.combine(target_day, time.max, zone)
        return start_day, end_day
    if report_type == "weekly":
        start_date = today - timedelta(days=today.weekday())
        end_date = start_date + timedelta(days=6)
        return datetime.combine(start_date, time.min, zone), datetime.combine(end_date, time.max, zone)
    if report_type in {"monthly", "summary"}:
        start_date = today.replace(day=1)
        if start_date.month == 12:
            next_month = date(start_date.year + 1, 1, 1)
        else:
            next_month = date(start_date.year, start_date.month + 1, 1)
        end_date = next_month - timedelta(days=1)
        return datetime.combine(start_date, time.min, zone), datetime.combine(end_date, time.max, zone)
    raise ValueError(f"Unsupported report type: {report_type}")


def build_title(report_type: str, start_at: datetime, end_at: datetime) -> str:
    labels = {
        "daily": "日报",
        "weekly": "周报",
        "monthly": "月报",
        "summary": "工作总结",
    }
    return f"{start_at:%Y-%m-%d} 至 {end_at:%Y-%m-%d} {labels[report_type]}"


def _build_period_label(report_type: str, start_at: datetime, end_at: datetime) -> str:
    if report_type == "daily":
        return f"{start_at:%Y-%m-%d} 日报"
    if report_type == "weekly":
        return f"{start_at:%Y-%m-%d} 至 {end_at:%Y-%m-%d} 周报"
    if report_type == "monthly":
        return f"{start_at:%Y-%m} 月报"
    return f"{start_at:%Y-%m-%d} 至 {end_at:%Y-%m-%d} 工作总结"


def _clip_text(value: str, limit: int) -> str:
    text = value.strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit].rstrip()}..."


def _extract_change_lines(diff_text: str, limit: int = MAX_CHANGE_LINES_PER_COMMIT) -> list[str]:
    lines: list[str] = []
    for raw_line in diff_text.splitlines():
        if not raw_line or raw_line.startswith(("diff --git", "index ", "@@", "--- ", "+++ ", "Binary files ")):
            continue
        if raw_line.startswith(("+", "-")) and not raw_line.startswith(("+++", "---")):
            content = raw_line[1:].strip()
            if not content:
                continue
            lines.append(_clip_text(content, MAX_CHANGE_LINE_LENGTH))
        if len(lines) >= limit:
            break
    return lines


def _build_commit_summary_payload(record: CommitRecord) -> dict:
    return {
        "repository": record.repository,
        "repository_path": record.repository_path,
        "commit_hash": record.commit_hash,
        "authored_at": record.authored_at,
        "subject": record.subject,
        "body": _clip_text(record.body, 400) if record.body else "",
        "changed_files": record.changed_files[:MAX_REPOSITORY_FILES],
        "change_lines": _extract_change_lines(record.diff_text),
        "diff_excerpt": _clip_text(record.diff_text, MAX_COMMIT_SUMMARY_DIFF_CHARS),
    }


def _build_commit_summary_fallback(record: CommitRecord) -> str:
    changed_files = record.changed_files[:3]
    file_segment = f"涉及 {', '.join(changed_files)}" if changed_files else "涉及相关代码"
    change_lines = _extract_change_lines(record.diff_text, limit=2)
    if change_lines:
        return f"{file_segment} 的提交已完成，重点调整了 {'；'.join(change_lines)}。"
    if record.subject:
        return f"{file_segment} 的提交已完成，主要工作可概括为：{_clip_text(record.subject, 80)}。"
    return f"{file_segment} 的提交已完成，已根据代码变更生成基础摘要。"


def _upsert_commit_summary(conn: sqlite3.Connection, record: CommitRecord, generated_summary: str, llm_note: str) -> None:
    conn.execute(
        """
        INSERT INTO commit_summaries (
            repository, repository_path, commit_hash, author_name, author_email,
            authored_at, subject, body, changed_files, diff_excerpt, generated_summary, llm_note
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(repository_path, commit_hash) DO UPDATE SET
            repository = excluded.repository,
            author_name = excluded.author_name,
            author_email = excluded.author_email,
            authored_at = excluded.authored_at,
            subject = excluded.subject,
            body = excluded.body,
            changed_files = excluded.changed_files,
            diff_excerpt = excluded.diff_excerpt,
            generated_summary = excluded.generated_summary,
            llm_note = excluded.llm_note,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            record.repository,
            record.repository_path,
            record.commit_hash,
            record.author_name,
            record.author_email,
            record.authored_at,
            record.subject,
            record.body,
            serialize_snapshot(record.changed_files[:MAX_REPOSITORY_FILES]),
            _clip_text(record.diff_text, MAX_COMMIT_SUMMARY_DIFF_CHARS),
            generated_summary,
            llm_note,
        ),
    )
    conn.commit()


def _load_commit_summary(conn: sqlite3.Connection, record: CommitRecord) -> dict | None:
    row = conn.execute(
        """
        SELECT * FROM commit_summaries
        WHERE repository_path = ? AND commit_hash = ?
        LIMIT 1
        """,
        (record.repository_path, record.commit_hash),
    ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["changed_files"] = json.loads(item["changed_files"])
    return item


def _ensure_commit_summary(conn: sqlite3.Connection, llm_client: LlmClient, record: CommitRecord) -> tuple[dict, bool]:
    existing = _load_commit_summary(conn, record)
    if existing and existing["generated_summary"].strip():
        return existing, False

    generated_summary, llm_note = llm_client.generate_commit_summary(_build_commit_summary_payload(record))
    final_summary = generated_summary or _build_commit_summary_fallback(record)
    _upsert_commit_summary(conn, record, final_summary, llm_note)
    stored = _load_commit_summary(conn, record)
    if not stored:
        raise RuntimeError(f"提交摘要保存失败：{record.repository} {record.commit_hash[:8]}")
    return stored, True


def _is_llm_timeout(message: str) -> bool:
    normalized = message.lower()
    return "timed out" in normalized or "timeout" in normalized or "超时" in normalized


def build_structured_summary(report_type: str, start_at: datetime, end_at: datetime, commit_items: list[dict]) -> dict:
    records = sorted(commit_items, key=lambda item: item["authored_at"])
    groups: dict[str, list[dict]] = defaultdict(list)
    for item in records:
        groups[item["repository"]].append(item)
    group_items = []
    for repo_name, repo_records in groups.items():
        changed_files: list[str] = []
        seen_files: set[str] = set()
        change_focus: list[str] = []
        for item in repo_records:
            for path in item["changed_files"]:
                if path not in seen_files:
                    seen_files.add(path)
                    changed_files.append(path)
            summary_text = item["generated_summary"].strip()
            if summary_text and summary_text not in change_focus:
                change_focus.append(summary_text)
            if len(change_focus) >= 8:
                break
        group_items.append(
            {
                "repository": repo_name,
                "count": len(repo_records),
                "changed_files": changed_files[:MAX_REPOSITORY_FILES],
                "changed_file_count": len(seen_files),
                "change_focus": change_focus[:8],
            }
        )

    return {
        "report_type": report_type,
        "period_label": _build_period_label(report_type, start_at, end_at),
        "period_start": start_at.isoformat(),
        "period_end": end_at.isoformat(),
        "period_start_date": start_at.strftime("%Y-%m-%d"),
        "period_end_date": end_at.strftime("%Y-%m-%d"),
        "commit_count": len(records),
        "repository_count": len(groups),
        "repositories": group_items,
        "commits": [
            {
                "repository": item["repository"],
                "commit_hash": item["commit_hash"],
                "authored_at": item["authored_at"],
                "changed_files": item["changed_files"][:MAX_REPOSITORY_FILES],
                "changed_file_count": len(item["changed_files"]),
                "subject_hint": item["subject"],
                "summary": item["generated_summary"],
            }
            for item in records
        ],
    }


def build_weekly_structured_summary(start_at: datetime, end_at: datetime, daily_reports: list[dict]) -> dict:
    ordered_reports = sorted(daily_reports, key=lambda item: item["period_start"])
    empty_count = sum(1 for item in ordered_reports if item["is_empty"])
    return {
        "report_type": "weekly",
        "period_label": _build_period_label("weekly", start_at, end_at),
        "period_start": start_at.isoformat(),
        "period_end": end_at.isoformat(),
        "period_start_date": start_at.strftime("%Y-%m-%d"),
        "period_end_date": end_at.strftime("%Y-%m-%d"),
        "daily_report_count": len(ordered_reports),
        "empty_daily_count": empty_count,
        "daily_reports": [
            {
                "date": item["period_start"][:10],
                "report_id": item["id"],
                "title": item["title"],
                "has_manual_edits": item["has_manual_edits"],
                "content_source": item["content_source"],
                "is_empty": item["is_empty"],
                "content": item["content"],
            }
            for item in ordered_reports
        ],
    }


def build_fallback_draft(report_type: str, structured_summary: dict) -> str:
    if report_type == "weekly" and "daily_reports" in structured_summary:
        if not structured_summary["daily_reports"]:
            return (
                f"### {structured_summary['period_label']}\n"
                "1. 当前周未找到可用日报记录。\n"
                "2. 建议先检查日报补齐流程是否执行成功。\n"
                "3. 如存在非代码工作或后续安排，可在周报中人工补充会议、联调、排障和计划信息。"
            )
        lines = [
            f"### {structured_summary['period_label']}",
            f"- 时间范围：{structured_summary['period_start']} ~ {structured_summary['period_end']}",
            f"- 已汇总日报：{structured_summary['daily_report_count']} 天",
            f"- 空日报：{structured_summary['empty_daily_count']} 天",
            "",
            "### 每日记录",
        ]
        for item in structured_summary["daily_reports"]:
            summary = _clip_text(item["content"], 180) if item["content"] else "当日无有效日报内容。"
            lines.append(f"- {item['date']}：{summary}")
        return "\n".join(lines)

    labels = {
        "daily": "今日工作",
        "weekly": "本周工作",
        "monthly": "本月工作",
        "summary": "本阶段工作总结",
    }
    if structured_summary["commit_count"] == 0:
        return (
            f"### {structured_summary['period_label']}\n"
            "1. 当前时间范围内未识别到本人有效提交记录。\n"
            "2. 如有实际工作进展，可在此基础上手动补充会议、联调、排障等非代码工作。\n"
            "3. 建议检查仓库路径、作者别名映射或提交邮箱配置是否正确。"
        )

    lines = [
        f"### {structured_summary['period_label']}",
        f"- 时间范围：{structured_summary['period_start']} ~ {structured_summary['period_end']}",
        f"- 涉及仓库：{structured_summary['repository_count']} 个",
        f"- 有效提交：{structured_summary['commit_count']} 条",
        "",
        f"### {labels[report_type]}",
    ]
    for repo in structured_summary["repositories"]:
        lines.append(f"- {repo['repository']}：完成 {repo['count']} 次提交")
        if repo["changed_files"]:
            lines.append(f"  - 主要改动文件：{', '.join(repo['changed_files'][:6])}")
        for change in repo["change_focus"][:4]:
            lines.append(f"  - 归纳摘要：{change}")
    lines.extend(
        [
            "",
            "### 风险与说明",
            "- 当前内容仅基于代码改动生成，适合先确认已完成事项；业务背景、协作沟通和后续计划建议人工补充。",
        ]
    )
    return "\n".join(lines)


def build_prompt(report_type: str, structured_summary: dict) -> str:
    if report_type == "weekly" and "daily_reports" in structured_summary:
        return (
            "请基于以下本周每日日报，输出一份中文周报，聚焦本周完成事项、重点进展和说明。\n"
            "要求：\n"
            "1. 仅以结构化输入中的每日日报内容为依据做汇总，不要回到 commit 维度重新推断。\n"
            "2. 输入的每日日报信息需要尽可能详尽地吸收进周报，优先保留关键事实、改动点和上下文，不要无依据地过度压缩。\n"
            "3. 优先合并同类事项，避免按日期机械复述；但若某天为空日报，可在说明中自然体现。\n"
            "4. 必须严格使用结构化输入中的 period_label、period_start_date、period_end_date 作为周报日期依据。\n"
            "5. 如果正文中需要写日期，请使用绝对日期，不要自行推断“本周”“昨天”等相对日期。\n"
            "6. 输出偏管理可读，语气专业、自然。\n"
            "7. 不要把代码提交直接推断成“风险问题”“下周计划”或其他承诺性表述；只有输入里有明确依据时，才写“说明”，否则省略该部分。\n\n"
            f"结构化输入：\n{json.dumps(structured_summary, ensure_ascii=False, indent=2)}"
        )

    prompt_goal = {
        "daily": "请输出一份简洁专业的中文日报，聚焦今日完成、变更摘要和说明。",
        "weekly": "请输出一份中文周报，聚焦本周完成、重点进展和说明。",
        "monthly": "请输出一份中文月报，聚焦本月成果、阶段进展和说明。",
        "summary": "请输出一份中文工作总结，聚焦完成事项、价值产出和说明。",
    }[report_type]
    return (
        f"{prompt_goal}\n"
        "要求：\n"
        "1. 以代码实际改动内容为主要依据，不要依赖 commit 标题做总结，更不要照抄 commit message。\n"
        "2. 输出偏管理可读，语气专业、自然。\n"
        "3. 对零散提交做主题合并，提炼成完成事项、模块变更和问题修复，避免逐条罗列 commit hash。\n"
        "4. 优先依据每条提交已经生成好的 summary 做汇总，可参考 changed_files 和 subject_hint 辅助理解，但不要回退成逐条抄写。\n"
        "5. 必须严格使用结构化输入中的 period_label、period_start_date、period_end_date 作为报告日期依据，不要自行推断今天、明天、本周或月份，也不要输出与输入不一致的日期。\n"
        "6. 如果需要在正文里写日期，请优先写绝对日期，例如 `2026-05-14`，不要写模型自行判断的相对日期。\n"
        "7. 如果日志为空，明确提示“当前周期未识别到有效提交，可补充非代码工作”。\n\n"
        "8. 不要仅凭 git 记录编造“风险阻塞”“风险问题”“明日计划”“下周计划”“下一阶段建议”等超出输入依据的内容；只有当输入中出现明确依据时，才补充“说明”，否则省略该部分。\n\n"
        f"结构化输入：\n{json.dumps(structured_summary, ensure_ascii=False, indent=2)}"
    )


def _fetch_aliases_by_repository(conn: sqlite3.Connection) -> dict[int, list[dict]]:
    rows = conn.execute("SELECT * FROM author_aliases ORDER BY id DESC").fetchall()
    items: dict[int, list[dict]] = {}
    for row in rows:
        alias = dict(row)
        items.setdefault(int(alias["repository_id"]), []).append(alias)
    return items


def _fetch_repositories(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM repositories WHERE is_enabled = 1 ORDER BY id DESC"
    ).fetchall()
    return [dict(row) for row in rows]


def _normalize_commit_summary_row(row: sqlite3.Row | dict) -> dict:
    item = dict(row)
    item["changed_files"] = json.loads(item["changed_files"]) if isinstance(item["changed_files"], str) else item["changed_files"]
    return item


def _normalize_report_row(row: sqlite3.Row | dict) -> dict:
    item = dict(row)
    item["auto_generated"] = bool(item["auto_generated"])
    item["has_manual_edits"] = bool(item.get("has_manual_edits", 0))
    item["source_snapshot"] = json.loads(item["source_snapshot"]) if isinstance(item["source_snapshot"], str) else item["source_snapshot"]
    return item


def _fetch_report_by_period(
    conn: sqlite3.Connection,
    report_type: str,
    start_at: datetime,
    end_at: datetime,
) -> dict | None:
    row = conn.execute(
        """
        SELECT * FROM reports
        WHERE report_type = ? AND period_start = ? AND period_end = ?
        LIMIT 1
        """,
        (report_type, start_at.isoformat(), end_at.isoformat()),
    ).fetchone()
    if not row:
        return None
    return _normalize_report_row(row)


def _store_report(
    conn: sqlite3.Connection,
    report_type: str,
    start_at: datetime,
    end_at: datetime,
    llm_client: LlmClient,
    structured_summary: dict,
    generated_content: str,
    notes: list[str],
    auto_generated: bool,
    overwrite_final: bool,
    prompt_version: str,
) -> dict:
    existing = _fetch_report_by_period(conn, report_type, start_at, end_at)
    content = generated_content
    has_manual_edits = 0
    if existing and existing["has_manual_edits"] and not overwrite_final:
        content = existing["content"]
        has_manual_edits = 1

    payload = (
        build_title(report_type, start_at, end_at),
        1 if auto_generated else 0,
        llm_client.model,
        prompt_version,
        serialize_snapshot(structured_summary),
        content,
        generated_content,
        has_manual_edits,
        "\n".join(notes),
        report_type,
        start_at.isoformat(),
        end_at.isoformat(),
    )

    if existing:
        conn.execute(
            """
            UPDATE reports
            SET title = ?, auto_generated = ?, llm_provider = 'openai-compatible',
                llm_model = ?, prompt_version = ?, source_snapshot = ?, content = ?,
                generated_content = ?, has_manual_edits = ?, generation_notes = ?, updated_at = CURRENT_TIMESTAMP
            WHERE report_type = ? AND period_start = ? AND period_end = ?
            """,
            payload,
        )
        report_id = int(existing["id"])
    else:
        cursor = conn.execute(
            """
            INSERT INTO reports (
                title, auto_generated, llm_provider, llm_model, prompt_version,
                source_snapshot, content, generated_content, has_manual_edits, generation_notes,
                report_type, period_start, period_end
            ) VALUES (?, ?, 'openai-compatible', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        report_id = int(cursor.lastrowid)

    return get_report(conn, report_id)


def _collect_commit_records(
    conn: sqlite3.Connection,
    start_at: datetime,
    end_at: datetime,
    update_progress: Callable[[str, str], None],
) -> tuple[list[CommitRecord], list[str]]:
    aliases_by_repository = _fetch_aliases_by_repository(conn)
    repositories = _fetch_repositories(conn)
    records: list[CommitRecord] = []
    errors: list[str] = []
    update_progress("scan", f"已加载 {len(repositories)} 个启用仓库，准备读取 git 提交。")
    for repository in repositories:
        update_progress("git_log", f"正在扫描仓库 {repository['name']} 的 git 提交记录。")
        try:
            repo_records = collect_commits(
                repository["name"],
                repository["path"],
                start_at.isoformat(),
                end_at.isoformat(),
                bool(repository["exclude_merge_commits"]),
                include_diff=True,
            )
        except GitRepositoryError as exc:
            errors.append(f"{repository['name']}: {exc}")
            continue

        repo_aliases = aliases_by_repository.get(int(repository["id"]), [])
        for record in repo_records:
            if author_matches(
                record.author_name,
                record.author_email,
                repo_aliases,
                bool(repository["exclude_bots"]),
            ):
                records.append(record)
    return records, errors


def list_commit_summaries(conn: sqlite3.Connection, authored_date: str | None = None, limit: int = 200) -> list[dict]:
    if authored_date:
        rows = conn.execute(
            """
            SELECT *
            FROM commit_summaries
            WHERE substr(authored_at, 1, 10) = ?
            ORDER BY datetime(authored_at) DESC, id DESC
            LIMIT ?
            """,
            (authored_date, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM commit_summaries
            ORDER BY datetime(authored_at) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [_normalize_commit_summary_row(row) for row in rows]


def generate_commit_summaries_for_period(
    conn: sqlite3.Connection,
    start_at: datetime,
    end_at: datetime,
    llm_client: LlmClient,
) -> dict:
    records, errors = _collect_commit_records(conn, start_at, end_at, lambda _stage, _detail: None)
    generated_count = 0
    summaries: list[dict] = []
    for record in records:
        stored_summary, created = _ensure_commit_summary(conn, llm_client, record)
        summaries.append(stored_summary)
        if created:
            generated_count += 1
    ordered = sorted(summaries, key=lambda item: item["authored_at"], reverse=True)
    return {
        "date": start_at.strftime("%Y-%m-%d"),
        "total_commits": len(records),
        "generated_count": generated_count,
        "reused_count": len(records) - generated_count,
        "errors": errors,
        "summaries": ordered,
    }


def _build_commit_based_report(
    conn: sqlite3.Connection,
    report_type: str,
    start_at: datetime,
    end_at: datetime,
    llm_client: LlmClient,
    auto_generated: bool,
    overwrite_final: bool,
    update_progress: Callable[[str, str], None],
) -> dict:
    records, errors = _collect_commit_records(conn, start_at, end_at, update_progress)

    commit_summary_items: list[dict] = []
    generated_count = 0
    fallback_summary_count = 0
    timeout_summary_count = 0
    update_progress("summarize", f"已识别 {len(records)} 条有效提交，正在生成新的提交摘要并写入本地记录。")
    for index, record in enumerate(records, start=1):
        update_progress("commit_summary", f"正在处理第 {index}/{len(records)} 条提交：{record.repository} {record.commit_hash[:8]}")
        stored_summary, created = _ensure_commit_summary(conn, llm_client, record)
        commit_summary_items.append(stored_summary)
        if created:
            generated_count += 1
            llm_note = stored_summary.get("llm_note", "")
            if llm_note and llm_note != "提交摘要生成成功。":
                fallback_summary_count += 1
                if _is_llm_timeout(llm_note):
                    timeout_summary_count += 1

    structured_summary = build_structured_summary(report_type, start_at, end_at, commit_summary_items)
    fallback = build_fallback_draft(report_type, structured_summary)
    update_progress("llm", f"提交摘要已整理完成（新增 {generated_count} 条，本地复用 {len(commit_summary_items) - generated_count} 条），正在生成报告内容。")
    report_prompt = build_prompt(report_type, structured_summary)
    ai_content, llm_note = llm_client.generate_report(
        report_prompt,
        metadata={
            "commit_count": structured_summary["commit_count"],
            "repository_count": structured_summary["repository_count"],
        },
    )
    generated_content = ai_content or fallback
    notes = [llm_note]
    notes.append(
        "提交流水线："
        f"共识别 {len(records)} 条有效提交；"
        f"新增摘要 {generated_count} 条；"
        f"复用本地摘要 {len(commit_summary_items) - generated_count} 条；"
        f"摘要兜底 {fallback_summary_count} 条；"
        f"其中超时 {timeout_summary_count} 条。"
    )
    notes.append(
        "上下文规模："
        f"报告 prompt 约 {len(report_prompt)} 个字符；"
        f"参与汇总仓库 {structured_summary['repository_count']} 个；"
        f"参与汇总提交 {structured_summary['commit_count']} 条。"
    )
    if errors:
        notes.append("部分仓库采集失败：" + "；".join(errors))

    update_progress("save", "正在保存报告记录。")
    return _store_report(
        conn=conn,
        report_type=report_type,
        start_at=start_at,
        end_at=end_at,
        llm_client=llm_client,
        structured_summary=structured_summary,
        generated_content=generated_content,
        notes=notes,
        auto_generated=auto_generated,
        overwrite_final=overwrite_final,
        prompt_version="v3-commit-summary-based",
    )


def _iter_daily_periods(start_at: datetime, end_at: datetime) -> list[tuple[datetime, datetime]]:
    current = start_at.date()
    end_date = end_at.date()
    periods: list[tuple[datetime, datetime]] = []
    while current <= end_date:
        periods.append(
            (
                datetime.combine(current, time.min, start_at.tzinfo),
                datetime.combine(current, time.max, start_at.tzinfo),
            )
        )
        current += timedelta(days=1)
    return periods


def _resolve_daily_report_content(report: dict) -> tuple[str, str]:
    if report["has_manual_edits"]:
        return report["content"], "content"
    return report["generated_content"], "generated_content"


def _ensure_daily_report_for_weekly(
    conn: sqlite3.Connection,
    daily_start: datetime,
    daily_end: datetime,
    timezone: str,
    llm_client: LlmClient,
    update_progress: Callable[[str, str], None],
) -> tuple[dict, bool]:
    existing = _fetch_report_by_period(conn, "daily", daily_start, daily_end)
    if existing:
        return existing, False

    update_progress("daily_backfill", f"周报补齐日报：正在生成 {daily_start:%Y-%m-%d} 的日报。")
    report = _build_commit_based_report(
        conn=conn,
        report_type="daily",
        start_at=daily_start,
        end_at=daily_end,
        llm_client=llm_client,
        auto_generated=True,
        overwrite_final=False,
        update_progress=lambda _stage, _detail: None,
    )
    return report, True


def _build_weekly_report(
    conn: sqlite3.Connection,
    start_at: datetime,
    end_at: datetime,
    timezone: str,
    llm_client: LlmClient,
    auto_generated: bool,
    overwrite_final: bool,
    update_progress: Callable[[str, str], None],
) -> dict:
    update_progress("prepare_daily_reports", "正在检查当前周的日报是否齐全。")
    daily_reports: list[dict] = []
    generated_daily_count = 0
    reused_daily_count = 0
    for daily_start, daily_end in _iter_daily_periods(start_at, end_at):
        report, created = _ensure_daily_report_for_weekly(
            conn=conn,
            daily_start=daily_start,
            daily_end=daily_end,
            timezone=timezone,
            llm_client=llm_client,
            update_progress=update_progress,
        )
        daily_reports.append(report)
        if created:
            generated_daily_count += 1
        else:
            reused_daily_count += 1

    update_progress("weekly_summary", f"本周 7 天日报已齐全（补生成 {generated_daily_count} 天，复用 {reused_daily_count} 天），正在汇总周报。")
    daily_items = []
    empty_daily_count = 0
    for report in daily_reports:
        content, content_source = _resolve_daily_report_content(report)
        source_snapshot = report["source_snapshot"]
        is_empty = bool(source_snapshot.get("commit_count", 0) == 0)
        if is_empty:
            empty_daily_count += 1
        daily_items.append(
            {
                "id": report["id"],
                "title": report["title"],
                "has_manual_edits": report["has_manual_edits"],
                "period_start": report["period_start"],
                "content_source": content_source,
                "content": content,
                "is_empty": is_empty,
            }
        )

    structured_summary = build_weekly_structured_summary(start_at, end_at, daily_items)
    fallback = build_fallback_draft("weekly", structured_summary)
    weekly_prompt = build_prompt("weekly", structured_summary)
    ai_content, llm_note = llm_client.generate_report(
        weekly_prompt,
        metadata={
            "commit_count": len(daily_items),
            "repository_count": 0,
        },
    )
    generated_content = ai_content or fallback
    notes = [llm_note]
    notes.append(
        "日报补齐："
        f"本周共检查 7 天；"
        f"补生成日报 {generated_daily_count} 天；"
        f"复用已有日报 {reused_daily_count} 天；"
        f"空日报 {empty_daily_count} 天。"
    )
    notes.append(
        "上下文规模："
        f"周报 prompt 约 {len(weekly_prompt)} 个字符；"
        f"参与汇总日报 {len(daily_items)} 天。"
    )
    update_progress("save", "正在保存周报记录。")
    return _store_report(
        conn=conn,
        report_type="weekly",
        start_at=start_at,
        end_at=end_at,
        llm_client=llm_client,
        structured_summary=structured_summary,
        generated_content=generated_content,
        notes=notes,
        auto_generated=auto_generated,
        overwrite_final=overwrite_final,
        prompt_version="v4-weekly-from-daily",
    )


def generate_report(
    conn: sqlite3.Connection,
    report_type: str,
    timezone: str,
    llm_client: LlmClient,
    period_start: str | None = None,
    period_end: str | None = None,
    auto_generated: bool = False,
    overwrite_final: bool = False,
    progress_callback: Callable[[str, str], None] | None = None,
) -> dict:
    def update_progress(stage: str, detail: str) -> None:
        if progress_callback:
            progress_callback(stage, detail)

    update_progress("prepare", "正在计算报告时间范围并加载基础数据。")
    start_at, end_at = compute_period(report_type, timezone, period_start, period_end)

    if report_type == "weekly":
        report = _build_weekly_report(
            conn=conn,
            start_at=start_at,
            end_at=end_at,
            timezone=timezone,
            llm_client=llm_client,
            auto_generated=auto_generated,
            overwrite_final=overwrite_final,
            update_progress=update_progress,
        )
    else:
        report = _build_commit_based_report(
            conn=conn,
            report_type=report_type,
            start_at=start_at,
            end_at=end_at,
            llm_client=llm_client,
            auto_generated=auto_generated,
            overwrite_final=overwrite_final,
            update_progress=update_progress,
        )

    update_progress("done", "报告已生成完成。")
    return report


def get_report(conn: sqlite3.Connection, report_id: int) -> dict:
    row = conn.execute("SELECT * FROM reports WHERE id = ?", (report_id,)).fetchone()
    if not row:
        raise KeyError(report_id)
    return _normalize_report_row(row)


def list_reports(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM reports ORDER BY datetime(created_at) DESC, id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [_normalize_report_row(row) for row in rows]
