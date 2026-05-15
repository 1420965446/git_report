from __future__ import annotations

import json
import socket
from typing import Any
from urllib.parse import urlparse
from urllib import error, request


def _normalize_base_url(base_url: str) -> str:
    return base_url.strip().rstrip("/")


def _build_chat_completions_url(base_url: str) -> str:
    normalized = _normalize_base_url(base_url)
    if normalized.endswith("/chat/completions"):
        return normalized
    if normalized.endswith("/v1"):
        return f"{normalized}/chat/completions"

    parsed = urlparse(normalized)
    path = parsed.path.rstrip("/")
    if not path:
        return f"{normalized}/v1/chat/completions"
    return f"{normalized}/chat/completions"


def _extract_status_code(message: str) -> int | None:
    if "HTTP " not in message:
        return None
    try:
        return int(message.split("HTTP ", 1)[1].split(" ", 1)[0])
    except (ValueError, IndexError):
        return None


def _build_http_diagnostic(status_code: int, detail: str, base_url: str, model: str) -> str:
    advice_map = {
        400: "请求格式或模型参数可能不被上游接受，请检查 Base URL 是否为 OpenAI 兼容接口，以及模型名是否正确。",
        401: "鉴权失败，请检查 API Key 是否有效、是否过期，或该网关是否要求其他认证头。",
        403: "请求被服务端拒绝，常见原因是网关风控、IP/地区限制、账号无权限，或该域名并不开放标准 API 调用。",
        404: "接口地址不存在，请确认 Base URL 末尾是否应为 `/v1`，以及该服务是否提供 `/chat/completions` 路径。",
        408: "上游响应超时，请稍后重试，或检查网络连通性。",
        429: "请求频率或额度受限，请检查账号余额、套餐限额，或稍后重试。",
        500: "上游服务内部错误，通常不是本地配置格式问题，可稍后重试。",
        502: "上游网关异常，可能是代理层或服务暂时不可用。",
        503: "上游服务当前不可用，可能在维护或过载。",
        504: "上游网关超时，通常是服务响应过慢或网络链路不稳定。",
    }
    advice = advice_map.get(status_code, "请检查网关兼容性、模型名、鉴权方式和服务端日志。")
    compact_detail = " ".join(detail.split())[:240] if detail else "无详细返回体。"
    return (
        f"诊断建议：{advice}\n"
        f"当前配置：Base URL={base_url or '-'}，Model={model or '-'}。\n"
        f"服务端返回：HTTP {status_code}，{compact_detail}"
    )


def _append_diagnostic(message: str, base_url: str, model: str) -> str:
    status_code = _extract_status_code(message)
    if status_code is None:
        return message
    detail = message.split(f"HTTP {status_code}", 1)[1].strip() if f"HTTP {status_code}" in message else ""
    return f"{message}\n\n{_build_http_diagnostic(status_code, detail, base_url, model)}"


class LlmClient:
    def __init__(
        self,
        base_url: str,
        model: str,
        api_key: str,
        report_timeout_seconds: int = 45,
        commit_summary_timeout_seconds: int = 30,
    ) -> None:
        self.base_url = _normalize_base_url(base_url)
        self.model = model
        self.api_key = api_key
        self.report_timeout_seconds = max(10, int(report_timeout_seconds))
        self.commit_summary_timeout_seconds = max(10, int(commit_summary_timeout_seconds))

    @property
    def configured(self) -> bool:
        return bool(self.base_url and self.model and self.api_key)

    @property
    def chat_completions_url(self) -> str:
        return _build_chat_completions_url(self.base_url)

    def _request_chat_completion(
        self,
        prompt: str,
        timeout: int = 45,
        purpose: str = "LLM 请求",
        metadata: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        if not self.configured:
            return None, "LLM 未配置，请先填写 Base URL、Model 和 API Key。"

        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "你是资深研发助理，负责把 git 工作记录整理成专业、简洁、可信的中文工作报告。",
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.3,
        }
        data = json.dumps(payload).encode("utf-8")
        http_request = request.Request(
            self.chat_completions_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
                # Some API gateways/WAFs block the default Python-urllib user agent.
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0 Safari/537.36",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            with request.urlopen(http_request, timeout=timeout) as response:
                body = json.loads(response.read().decode("utf-8"))
                return body, f"LLM 请求成功，HTTP {getattr(response, 'status', 200)}。"
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raw_message = f"LLM 请求失败: HTTP {exc.code} {detail}".strip()
            return None, _append_diagnostic(raw_message, self.chat_completions_url, self.model)
        except socket.timeout:
            diagnostic = self._build_runtime_diagnostic(purpose, timeout, prompt, metadata)
            return None, f"LLM 请求失败: The read operation timed out\n\n{diagnostic}"
        except Exception as exc:  # noqa: BLE001
            diagnostic = self._build_runtime_diagnostic(purpose, timeout, prompt, metadata)
            return None, f"LLM 请求失败: {exc}\n\n{diagnostic}"

    def _build_runtime_diagnostic(
        self,
        purpose: str,
        timeout: int,
        prompt: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        prompt_chars = len(prompt)
        base_message = (
            f"诊断建议：当前阶段为“{purpose}”，本次超时阈值 {timeout} 秒，"
            f"请求体 prompt 约 {prompt_chars} 个字符。"
        )
        extras: list[str] = []
        if metadata:
            if metadata.get("commit_hash"):
                extras.append(f"提交={metadata['commit_hash']}")
            if metadata.get("repository"):
                extras.append(f"仓库={metadata['repository']}")
            if metadata.get("commit_count") is not None:
                extras.append(f"提交数={metadata['commit_count']}")
            if metadata.get("repository_count") is not None:
                extras.append(f"仓库数={metadata['repository_count']}")
        guidance = (
            "若连接测试正常但这里超时，通常说明本阶段输入偏大或上游模型响应较慢；"
            "可缩短摘要内容、减少提交量，或提高对应超时配置。"
        )
        detail_line = f"上下文信息：{'，'.join(extras)}。" if extras else ""
        return "\n".join(item for item in [base_message, detail_line, guidance] if item)

    def generate_report(self, prompt: str, metadata: dict[str, Any] | None = None) -> tuple[str | None, str]:
        body, message = self._request_chat_completion(
            prompt,
            timeout=self.report_timeout_seconds,
            purpose="日报生成",
            metadata=metadata,
        )
        if body is None:
            if message.startswith("LLM 未配置"):
                return None, "LLM 未配置，已返回规则生成内容。"
            return None, message

        try:
            content = body["choices"][0]["message"]["content"].strip()
        except (KeyError, IndexError, TypeError):
            return None, "LLM 返回结构不符合预期，已返回规则生成内容。"
        return content, "LLM 生成成功。"

    def generate_commit_summary(self, commit_payload: dict[str, Any]) -> tuple[str | None, str]:
        prompt = (
            "请根据以下单条 git 提交信息，生成一条新的中文 commit 信息。\n"
            "要求：\n"
            "1. 优先依据代码改动内容、改动文件和 diff 摘要判断真实工作，不要照抄原始 subject。\n"
            "2. 输出 1 到 2 句话，总结这次提交完成了什么，必要时补充影响范围或修复目标。\n"
            "3. 语言专业、具体、简洁，不要带序号、标题、markdown。\n"
            "4. 如果信息不足，也要基于现有内容给出尽可能可信的概括。\n\n"
            f"提交信息：\n{json.dumps(commit_payload, ensure_ascii=False, indent=2)}"
        )
        body, message = self._request_chat_completion(
            prompt,
            timeout=self.commit_summary_timeout_seconds,
            purpose="提交摘要生成",
            metadata={
                "repository": commit_payload.get("repository", ""),
                "commit_hash": str(commit_payload.get("commit_hash", ""))[:8],
            },
        )
        if body is None:
            if message.startswith("LLM 未配置"):
                return None, "LLM 未配置，已返回规则生成摘要。"
            return None, message

        try:
            content = body["choices"][0]["message"]["content"].strip()
        except (KeyError, IndexError, TypeError):
            return None, "LLM 返回结构不符合预期，已返回规则生成摘要。"
        return content, "提交摘要生成成功。"

    def test_connection(self) -> tuple[bool, str, int | None]:
        body, message = self._request_chat_completion(
            "请仅回复“连接成功”。",
            timeout=20,
            purpose="连接测试",
        )
        if body is None:
            status_code = _extract_status_code(message)
            return False, message, status_code

        try:
            content = body["choices"][0]["message"]["content"].strip()
        except (KeyError, IndexError, TypeError):
            return False, "LLM 已响应，但返回结构不符合预期。", None
        return True, f"连接成功，请求地址：{self.chat_completions_url}，模型已返回内容：{content}", 200
