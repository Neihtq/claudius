import asyncio
from abc import ABC, abstractmethod
from pathlib import Path
from urllib.parse import urlparse


class AttachmentStore(ABC):
    @abstractmethod
    async def write(self, key: str, data: bytes) -> None: ...

    @abstractmethod
    async def read(self, key: str) -> bytes: ...

    @abstractmethod
    async def delete(self, key: str) -> None: ...

    @abstractmethod
    def describe(self) -> str: ...


class LocalAttachmentStore(AttachmentStore):
    def __init__(self, base_path: str | Path):
        self._base = Path(base_path)

    async def write(self, key: str, data: bytes) -> None:
        path = self._base / key
        await asyncio.to_thread(lambda: path.parent.mkdir(parents=True, exist_ok=True))
        await asyncio.to_thread(path.write_bytes, data)

    async def read(self, key: str) -> bytes:
        return await asyncio.to_thread((self._base / key).read_bytes)

    async def delete(self, key: str) -> None:
        path = self._base / key
        if await asyncio.to_thread(path.exists):
            await asyncio.to_thread(path.unlink)

    def describe(self) -> str:
        return f"local  path={self._base}"


class S3AttachmentStore(AttachmentStore):
    def __init__(self, bucket: str, prefix: str = ""):
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def _s3_key(self, key: str) -> str:
        return f"{self._prefix}/{key}" if self._prefix else key

    async def write(self, key: str, data: bytes) -> None:
        import boto3
        client = boto3.client("s3")
        await asyncio.to_thread(
            client.put_object, Bucket=self._bucket, Key=self._s3_key(key), Body=data
        )

    async def read(self, key: str) -> bytes:
        import boto3
        client = boto3.client("s3")
        resp = await asyncio.to_thread(
            client.get_object, Bucket=self._bucket, Key=self._s3_key(key)
        )
        return resp["Body"].read()

    async def delete(self, key: str) -> None:
        import boto3
        client = boto3.client("s3")
        await asyncio.to_thread(
            client.delete_object, Bucket=self._bucket, Key=self._s3_key(key)
        )

    def describe(self) -> str:
        location = f"{self._bucket}/{self._prefix}" if self._prefix else self._bucket
        return f"s3     bucket={location}"


def create_store(uri: str) -> AttachmentStore:
    """Parse 'file:///path' or 's3://bucket/prefix' into an AttachmentStore."""
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return LocalAttachmentStore(parsed.path)
    if parsed.scheme == "s3":
        return S3AttachmentStore(bucket=parsed.netloc, prefix=parsed.path.lstrip("/"))
    raise ValueError(f"Unknown attachment store URI scheme: {parsed.scheme!r} in {uri!r}")
