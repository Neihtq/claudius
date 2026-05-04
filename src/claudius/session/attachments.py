import asyncio
import os
from pathlib import Path

import httpx
from loguru import logger


async def sync_workspace_attachments(
    *,
    callback_url: str,
    session_id: str,
    session_token: str,
    workspace_path: str,
) -> int:
    """Mirror remote session attachments into the local workspace."""
    if not callback_url or not session_id or not session_token:
        return 0

    base = callback_url.rstrip("/")
    headers = {"Authorization": f"Bearer {session_token}"}
    attachments_dir = Path(workspace_path) / "attachments"

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                f"{base}/sessions/{session_id}/conversation",
                headers=headers,
                timeout=30.0,
            )
            resp.raise_for_status()
        except Exception as exc:
            logger.warning(
                "attachment sync skipped session_id={} error={}",
                session_id,
                exc,
            )
            return 0

        downloaded = 0
        for entry in resp.json():
            for att in entry.get("attachments", []):
                message_id = att.get("message_id")
                filename = att.get("filename")
                if not isinstance(message_id, str) or not isinstance(filename, str):
                    continue

                local_path = attachments_dir / message_id / filename
                if await asyncio.to_thread(local_path.exists):
                    continue

                try:
                    file_resp = await client.get(
                        (
                            f"{base}/sessions/{session_id}/messages/"
                            f"{message_id}/attachments/{filename}"
                        ),
                        headers=headers,
                        timeout=60.0,
                    )
                    file_resp.raise_for_status()
                except Exception as exc:
                    logger.warning(
                        "attachment download failed session_id={} message_id={} filename={} error={}",
                        session_id,
                        message_id,
                        filename,
                        exc,
                    )
                    continue

                await asyncio.to_thread(lambda: local_path.parent.mkdir(parents=True, exist_ok=True))
                await asyncio.to_thread(local_path.write_bytes, file_resp.content)
                downloaded += 1

    if downloaded:
        logger.info(
            "synced {} attachment(s) into {}",
            downloaded,
            os.fspath(attachments_dir),
        )
    return downloaded
