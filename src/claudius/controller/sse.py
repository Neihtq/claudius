import asyncio
import json
from collections import defaultdict


class SSEBroker:
    def __init__(self):
        self._queues: dict[str, list[asyncio.Queue]] = defaultdict(list)

    def close(self) -> None:
        """Signal all active SSE streams to exit by sending a None sentinel."""
        for queues in self._queues.values():
            for q in queues:
                q.put_nowait(None)

    async def publish(self, session_id: str, event: dict) -> None:
        data = json.dumps(event)
        for q in list(self._queues[session_id]):
            await q.put(data)

    def subscribe(self, session_id: str, q: asyncio.Queue) -> None:
        self._queues[session_id].append(q)

    def unsubscribe(self, session_id: str, q: asyncio.Queue) -> None:
        try:
            self._queues[session_id].remove(q)
        except ValueError:
            pass
