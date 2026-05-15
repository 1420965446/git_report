from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


BOT_MARKERS = ("[bot]", "bot@", "noreply", "github-actions", "jenkins", "dependabot")


@dataclass(slots=True)
class CommitRecord:
    repository: str
    repository_path: str
    commit_hash: str
    author_name: str
    author_email: str
    authored_at: str
    subject: str
    body: str
    changed_files: list[str]
    diff_text: str


class GitRepositoryError(RuntimeError):
    pass


def _decode_git_output(payload: bytes | None) -> str:
    if not payload:
        return ""
    for encoding in ("utf-8", "gb18030"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode("utf-8", errors="replace")


def _run_git_command(command: list[str]) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(command, capture_output=True, text=False, check=False)
    except FileNotFoundError as exc:
        raise GitRepositoryError("未找到 git 命令") from exc


def _parse_changed_files(diff_text: str) -> list[str]:
    files: list[str] = []
    seen: set[str] = set()
    for line in diff_text.splitlines():
        if not line.startswith("diff --git "):
            continue
        parts = line.split(" ")
        if len(parts) < 4:
            continue
        raw_path = parts[3]
        if raw_path.startswith("b/"):
            raw_path = raw_path[2:]
        if raw_path not in seen:
            seen.add(raw_path)
            files.append(raw_path)
    return files


def _collect_commit_diff(repo_path: str, commit_hash: str) -> tuple[list[str], str]:
    command = [
        "git",
        "-C",
        repo_path,
        "show",
        commit_hash,
        "--format=",
        "--patch",
        "--stat=200,120",
        "--unified=3",
        "--find-renames",
        "--encoding=UTF-8",
    ]
    result = _run_git_command(command)
    if result.returncode != 0:
        stderr = (_decode_git_output(result.stderr) or _decode_git_output(result.stdout)).strip()
        raise GitRepositoryError(stderr or f"读取提交 {commit_hash[:8]} 的改动详情失败")
    diff_text = _decode_git_output(result.stdout).strip()
    return _parse_changed_files(diff_text), diff_text


def validate_git_repository(path: str) -> tuple[bool, str]:
    repo_path = Path(path).expanduser()
    if not repo_path.exists():
        return False, "路径不存在"
    if not repo_path.is_dir():
        return False, "路径不是目录"
    try:
        result = _run_git_command(["git", "-C", str(repo_path), "rev-parse", "--is-inside-work-tree"])
    except GitRepositoryError as exc:
        return False, str(exc)
    if result.returncode != 0:
        stderr = (_decode_git_output(result.stderr) or _decode_git_output(result.stdout)).strip()
        return False, stderr or "不是有效的 git 仓库"
    return True, "仓库可用"


def resolve_repository_path(path: str) -> str:
    return str(Path(path).expanduser().resolve())


def collect_commits(
    repo_name: str,
    repo_path: str,
    since_iso: str,
    until_iso: str,
    exclude_merge_commits: bool,
    include_diff: bool = False,
) -> list[CommitRecord]:
    ok, message = validate_git_repository(repo_path)
    if not ok:
        raise GitRepositoryError(message)

    format_string = "%H%x1f%an%x1f%ae%x1f%aI%x1f%s%x1f%b%x1e"
    command = [
        "git",
        "-C",
        repo_path,
        "log",
        f"--since={since_iso}",
        f"--until={until_iso}",
        f"--pretty=format:{format_string}",
        "--date=iso-strict",
    ]
    if exclude_merge_commits:
        command.append("--no-merges")
    command.append("--encoding=UTF-8")

    result = _run_git_command(command)
    if result.returncode != 0:
        stderr = (_decode_git_output(result.stderr) or _decode_git_output(result.stdout)).strip()
        raise GitRepositoryError(stderr or "读取 git 日志失败")

    stdout = _decode_git_output(result.stdout)
    records: list[CommitRecord] = []
    for chunk in stdout.split("\x1e"):
        entry = chunk.strip("\r\n")
        if not entry:
            continue
        parts = entry.split("\x1f")
        if len(parts) != 6:
            continue
        records.append(
            CommitRecord(
                repository=repo_name,
                repository_path=repo_path,
                commit_hash=parts[0],
                author_name=parts[1],
                author_email=parts[2],
                authored_at=parts[3],
                subject=parts[4].strip(),
                body=parts[5].strip(),
                changed_files=[],
                diff_text="",
            )
        )
    if include_diff:
        for record in records:
            record.changed_files, record.diff_text = _collect_commit_diff(repo_path, record.commit_hash)
    return records


def collect_recent_commits(
    repo_name: str,
    repo_path: str,
    limit: int = 5,
    exclude_merge_commits: bool = False,
    include_diff: bool = False,
) -> list[CommitRecord]:
    ok, message = validate_git_repository(repo_path)
    if not ok:
        raise GitRepositoryError(message)

    format_string = "%H%x1f%an%x1f%ae%x1f%aI%x1f%s%x1f%b%x1e"
    command = [
        "git",
        "-C",
        repo_path,
        "log",
        f"-n{limit}",
        f"--pretty=format:{format_string}",
        "--date=iso-strict",
        "--encoding=UTF-8",
    ]
    if exclude_merge_commits:
        command.append("--no-merges")

    result = _run_git_command(command)
    if result.returncode != 0:
        stderr = (_decode_git_output(result.stderr) or _decode_git_output(result.stdout)).strip()
        raise GitRepositoryError(stderr or "读取最近提交失败")

    stdout = _decode_git_output(result.stdout)
    records: list[CommitRecord] = []
    for chunk in stdout.split("\x1e"):
        entry = chunk.strip("\r\n")
        if not entry:
            continue
        parts = entry.split("\x1f")
        if len(parts) != 6:
            continue
        records.append(
            CommitRecord(
                repository=repo_name,
                repository_path=repo_path,
                commit_hash=parts[0],
                author_name=parts[1],
                author_email=parts[2],
                authored_at=parts[3],
                subject=parts[4].strip(),
                body=parts[5].strip(),
                changed_files=[],
                diff_text="",
            )
        )
    if include_diff:
        for record in records:
            record.changed_files, record.diff_text = _collect_commit_diff(repo_path, record.commit_hash)
    return records


def author_matches(
    author_name: str,
    author_email: str,
    alias_rows: list[dict],
    exclude_bots: bool,
) -> bool:
    normalized_name = author_name.strip().lower()
    normalized_email = author_email.strip().lower()
    if exclude_bots and any(marker in normalized_name or marker in normalized_email for marker in BOT_MARKERS):
        return False
    for alias in alias_rows:
        alias_name = alias["alias_name"].strip().lower()
        alias_email = alias["alias_email"].strip().lower()
        canonical_name = alias["canonical_name"].strip().lower()
        if alias_name and normalized_name == alias_name:
            return True
        if alias_email and normalized_email == alias_email:
            return True
        if canonical_name and normalized_name == canonical_name:
            return True
    return False


def serialize_snapshot(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def parse_commit_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
