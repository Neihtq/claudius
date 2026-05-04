from pathlib import Path

import pytest

from claudius.session.attachments import sync_workspace_attachments


class _FakeResponse:
    def __init__(self, *, json_data=None, content=b"", should_raise=False):
        self._json_data = json_data
        self.content = content
        self._should_raise = should_raise

    def raise_for_status(self):
        if self._should_raise:
            raise RuntimeError("boom")

    def json(self):
        return self._json_data


class _FakeClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.requests = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, *, headers, timeout):
        self.requests.append({"url": url, "headers": headers, "timeout": timeout})
        return self._responses.pop(0)


@pytest.mark.asyncio
async def test_sync_workspace_attachments_downloads_missing_files(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    existing = workspace / "attachments" / "msg-1" / "existing.txt"
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_text("already here")

    responses = [
        _FakeResponse(json_data=[
            {
                "attachments": [
                    {"message_id": "msg-1", "filename": "existing.txt", "content_type": "text/plain"},
                    {"message_id": "msg-2", "filename": "new.pdf", "content_type": "application/pdf"},
                ]
            }
        ]),
        _FakeResponse(content=b"%PDF-1.7"),
    ]
    client = _FakeClient(responses)
    monkeypatch.setattr(
        "claudius.session.attachments.httpx.AsyncClient",
        lambda: client,
    )

    downloaded = await sync_workspace_attachments(
        callback_url="http://controller",
        session_id="sess-1",
        session_token="token-123",
        workspace_path=str(workspace),
    )

    assert downloaded == 1
    assert existing.read_text() == "already here"
    assert (workspace / "attachments" / "msg-2" / "new.pdf").read_bytes() == b"%PDF-1.7"
    assert len(client.requests) == 2


@pytest.mark.asyncio
async def test_sync_workspace_attachments_noops_without_callback(tmp_path):
    downloaded = await sync_workspace_attachments(
        callback_url="",
        session_id="sess-1",
        session_token="token-123",
        workspace_path=str(tmp_path),
    )

    assert downloaded == 0
    assert not (Path(tmp_path) / "attachments").exists()
