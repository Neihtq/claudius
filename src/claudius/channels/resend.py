import base64
import importlib
import json
import re
from datetime import datetime, timezone
from email.utils import parseaddr

import httpx
from fastapi import HTTPException, Request
from loguru import logger

from claudius.channels.base import AbstractChannel
from claudius.models import Attachment, InboundMessage

RESEND_API_URL = "https://api.resend.com"
_MESSAGE_ID_RE = re.compile(r"<([^>]+)>")
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def _normalize_address(value: str) -> str:
    _, address = parseaddr(value)
    return address or value.strip()


def _normalize_addresses(values: object) -> list[str]:
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, list):
        return []
    normalized: list[str] = []
    for item in values:
        if isinstance(item, str):
            address = _normalize_address(item)
        elif isinstance(item, dict):
            address = _normalize_address(str(item.get("email") or item.get("address") or ""))
        else:
            address = ""
        if address:
            normalized.append(address)
    return normalized


def _parse_header_values(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
            except Exception:
                parsed = None
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        return [stripped]
    return []


def _extract_header(headers: object, name: str) -> list[str]:
    target = name.lower()
    if isinstance(headers, dict):
        for key, value in headers.items():
            if str(key).lower() == target:
                return _parse_header_values(value)
        return []
    if isinstance(headers, list):
        values: list[str] = []
        for item in headers:
            if not isinstance(item, dict):
                continue
            if str(item.get("name", "")).lower() != target:
                continue
            values.extend(_parse_header_values(item.get("value")))
        return values
    return []


def _extract_message_ids(values: list[str]) -> list[str]:
    ids: list[str] = []
    for value in values:
        matches = _MESSAGE_ID_RE.findall(value)
        if matches:
            ids.extend(match.strip() for match in matches if match.strip())
            continue
        for token in value.split():
            token = token.strip().strip("<>")
            if token:
                ids.append(token)
    return ids


def _extract_body(email: dict, fallback: dict) -> str:
    for source in (email, fallback):
        text = source.get("text")
        if isinstance(text, str) and text.strip():
            return text
        html = source.get("html")
        if isinstance(html, str) and html.strip():
            return html
    return ""


def _markdown_to_html(body: str) -> str:
    try:
        markdown = importlib.import_module("markdown")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Markdown email rendering requires the 'markdown' package to be installed."
        ) from exc
    return markdown.markdown(
        body,
        extensions=["fenced_code", "tables", "sane_lists", "nl2br"],
    )


async def _lookup_received_email_id_by_message_id(
    client: httpx.AsyncClient,
    auth_headers: dict[str, str],
    message_id: str,
) -> str | None:
    resp = await client.get(
        f"{RESEND_API_URL}/emails/receiving",
        headers=auth_headers,
    )
    resp.raise_for_status()
    payload = resp.json()
    entries = payload.get("data", []) if isinstance(payload, dict) else []
    target = message_id.strip().strip("<>")
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        candidate = str(entry.get("message_id") or "").strip().strip("<>")
        if candidate == target:
            entry_id = str(entry.get("id") or "").strip()
            return entry_id or None
    return None


class ResendChannel(AbstractChannel):
    def __init__(self, api_key: str, from_address: str):
        self._api_key = api_key
        self._from_address = from_address

    async def parse_webhook(self, request: Request) -> InboundMessage:
        payload = await request.json()
        data = payload.get("data", payload)
        email_id = str(data.get("email_id", "")).strip()

        email = data
        if email_id:
            if not self._api_key.strip():
                fallback_body = str(data.get("text") or data.get("html") or "")
                if fallback_body:
                    email = {**data, "text": fallback_body}
                else:
                    raise HTTPException(
                        status_code=503,
                        detail=(
                            "Resend inbound body lookup requires RESEND_API_KEY to be set. "
                            "The webhook only included metadata for this email."
                        ),
                    )
            else:
                auth_headers = {"Authorization": f"Bearer {self._api_key}"}
                async with httpx.AsyncClient() as client:
                    resp = await client.get(
                        f"{RESEND_API_URL}/emails/receiving/{email_id}",
                        headers=auth_headers,
                    )
                    resp.raise_for_status()
                    payload = resp.json()
                    email = payload.get("data", payload) if isinstance(payload, dict) else {}

        sender = _normalize_address(str(email.get("from") or data.get("from") or ""))
        recipients = _normalize_addresses(email.get("to") or data.get("to"))
        subject = email.get("subject") or data.get("subject")
        body = _extract_body(email, data)

        headers = email.get("headers") or data.get("headers") or {}
        references = _extract_message_ids(_extract_header(headers, "references"))
        in_reply_to = _extract_message_ids(_extract_header(headers, "in-reply-to"))
        current_message_id = str(email.get("message_id") or data.get("message_id") or "").strip().strip("<>")
        root_message_id = (references[0] if references else (in_reply_to[0] if in_reply_to else current_message_id))
        thread_id = root_message_id or email_id

        if email_id and self._api_key.strip() and root_message_id:
            auth_headers = {"Authorization": f"Bearer {self._api_key}"}
            async with httpx.AsyncClient() as client:
                root_received_email_id = await _lookup_received_email_id_by_message_id(
                    client,
                    auth_headers,
                    root_message_id,
                )
            if root_received_email_id:
                thread_id = root_received_email_id

        return InboundMessage(
            channel="email",
            sender=sender,
            recipients=recipients,
            thread_id=thread_id,
            subject=subject,
            body=body,
            attachments=[],
            received_at=datetime.now(timezone.utc),
        )

    async def send_message(
        self,
        to: str,
        body: str,
        thread_id: str,
        attachments: list[Attachment] | None = None,
    ) -> None:
        request_headers = {"Authorization": f"Bearer {self._api_key}"}
        payload = {
            "from": self._from_address,
            "to": [to],
            "subject": "Re: (claudius)",
            "text": body,
            "html": _markdown_to_html(body),
            "attachments": [
                {
                    "filename": attachment.filename,
                    "content": base64.b64encode(attachment.data).decode("ascii"),
                }
                for attachment in (attachments or [])
            ],
        }

        message_id = thread_id.strip().strip("<>")
        async with httpx.AsyncClient() as client:
            if self._api_key.strip() and _UUID_RE.match(message_id):
                try:
                    resp = await client.get(
                        f"{RESEND_API_URL}/emails/receiving/{message_id}",
                        headers=request_headers,
                    )
                    resp.raise_for_status()
                    received = resp.json()
                    email = received.get("data", received) if isinstance(received, dict) else {}
                    original_message_id = str(email.get("message_id") or "").strip()
                    if original_message_id:
                        payload["headers"] = {
                            "In-Reply-To": original_message_id,
                            "References": original_message_id,
                        }
                    original_subject = str(email.get("subject") or "").strip()
                    if original_subject:
                        payload["subject"] = (
                            original_subject
                            if original_subject.lower().startswith("re:")
                            else f"Re: {original_subject}"
                        )
                except Exception as exc:
                    logger.warning(
                        "failed to resolve resend thread metadata thread_id={} error={}",
                        thread_id,
                        exc,
                    )
            elif message_id and "@" in message_id:
                payload["headers"] = {
                    "In-Reply-To": f"<{message_id}>",
                    "References": f"<{message_id}>",
                }

            resp = await client.post(
                f"{RESEND_API_URL}/emails",
                headers=request_headers,
                json=payload,
            )
            if resp.is_error:
                logger.error(
                    "resend send failed status={} body={}",
                    resp.status_code,
                    resp.text,
                )
            resp.raise_for_status()
