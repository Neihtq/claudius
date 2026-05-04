import asyncio
import json
import pytest
from claudius.controller.sse import SSEBroker


@pytest.mark.asyncio
async def test_publish_delivers_to_subscriber():
    broker = SSEBroker()
    q: asyncio.Queue = asyncio.Queue()
    broker.subscribe("sess-1", q)

    await broker.publish("sess-1", {"type": "ping"})

    data = await asyncio.wait_for(q.get(), timeout=1.0)
    assert json.loads(data) == {"type": "ping"}


@pytest.mark.asyncio
async def test_unsubscribe_removes_queue():
    broker = SSEBroker()
    q: asyncio.Queue = asyncio.Queue()
    broker.subscribe("sess-1", q)
    broker.unsubscribe("sess-1", q)

    await broker.publish("sess-1", {"type": "ping"})

    assert q.empty()


@pytest.mark.asyncio
async def test_publish_to_multiple_subscribers():
    broker = SSEBroker()
    q1: asyncio.Queue = asyncio.Queue()
    q2: asyncio.Queue = asyncio.Queue()
    broker.subscribe("sess-1", q1)
    broker.subscribe("sess-1", q2)

    await broker.publish("sess-1", {"type": "message", "body": "hi"})

    d1 = await asyncio.wait_for(q1.get(), timeout=1.0)
    d2 = await asyncio.wait_for(q2.get(), timeout=1.0)
    assert json.loads(d1)["body"] == "hi"
    assert json.loads(d2)["body"] == "hi"


@pytest.mark.asyncio
async def test_publish_to_wrong_session_does_not_deliver():
    broker = SSEBroker()
    q: asyncio.Queue = asyncio.Queue()
    broker.subscribe("sess-A", q)

    await broker.publish("sess-B", {"type": "ping"})

    assert q.empty()
