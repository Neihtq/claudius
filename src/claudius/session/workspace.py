from pathlib import Path


class Workspace:
    def __init__(self, base_path: str | Path, session_id: str):
        self._base = Path(base_path)
        self._session_id = session_id

    @property
    def path(self) -> Path:
        return self._base / self._session_id

    def setup(self) -> None:
        self.path.mkdir(parents=True, exist_ok=True)

    def archive(self) -> None:
        """Stub: upload workspace to S3. No-op until S3 support is added."""

    def restore(self) -> None:
        """Stub: download workspace from S3. No-op until S3 support is added."""
