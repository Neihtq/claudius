import aiosqlite
from datetime import datetime, timezone
import json
from pathlib import Path
import uuid
from claudius.models import (
    ClaudeSummary,
    Execution,
    ExecutionContainer,
    ExecutionPhase,
    LogLine,
    Session,
    SessionState,
)

_CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    thread_id TEXT UNIQUE NOT NULL,
    channel TEXT NOT NULL,
    workflow_name TEXT NOT NULL,
    state TEXT NOT NULL,
    workspace_path TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_message_at TEXT NOT NULL,
    last_execution_result TEXT,
    channel_metadata_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS executions (
    execution_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    worker_address TEXT,
    started_at TEXT NOT NULL,
    halted_at TEXT,
    halt_reason TEXT,
    phase TEXT NOT NULL DEFAULT 'starting',
    runtime_container_json TEXT,
    worker_container_json TEXT,
    exit_code INTEGER,
    claude_session_id TEXT,
    claude_num_turns INTEGER,
    claude_duration_ms INTEGER,
    claude_total_cost_usd REAL,
    claude_input_tokens INTEGER,
    claude_output_tokens INTEGER,
    claude_cache_creation_input_tokens INTEGER,
    claude_cache_read_input_tokens INTEGER,
    claude_total_tokens INTEGER
);
CREATE TABLE IF NOT EXISTS messages (
    message_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    direction TEXT NOT NULL,
    body TEXT NOT NULL,
    received_at TEXT NOT NULL,
    attachments TEXT NOT NULL DEFAULT '[]',
    sender TEXT NOT NULL DEFAULT '',
    acknowledged_at TEXT,
    delivery_error TEXT
);
CREATE TABLE IF NOT EXISTS execution_logs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL REFERENCES executions(execution_id),
    logged_at    TEXT NOT NULL,
    stream       TEXT NOT NULL CHECK(stream IN ('stdout', 'stderr')),
    body         TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS proxy_logs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL REFERENCES executions(execution_id),
    logged_at    TEXT NOT NULL,
    stage        TEXT NOT NULL,
    method       TEXT,
    path         TEXT,
    upstream_url TEXT,
    status_code  INTEGER,
    content_type TEXT,
    body         TEXT NOT NULL,
    meta         TEXT NOT NULL DEFAULT '{}',
    input_tokens INTEGER,
    output_tokens INTEGER,
    cache_creation_input_tokens INTEGER,
    cache_read_input_tokens INTEGER,
    total_tokens INTEGER,
    total_cost_usd REAL
);
CREATE TABLE IF NOT EXISTS conversation_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id  TEXT NOT NULL REFERENCES executions(execution_id),
    logged_at     TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    source        TEXT NOT NULL CHECK(source IN ('local', 'claude')),
    event_type    TEXT NOT NULL,
    event_subtype TEXT,
    payload_json  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS attachments (
    attachment_id TEXT PRIMARY KEY,
    message_id    TEXT NOT NULL REFERENCES messages(message_id),
    filename      TEXT NOT NULL,
    content_type  TEXT NOT NULL,
    storage_key   TEXT NOT NULL
);
"""


def _dt(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s) if s else None


def _summary_from_row(row) -> ClaudeSummary:
    return ClaudeSummary(
        executions_count=int(row["executions_count"] or 0),
        completed_executions_count=int(row["completed_executions_count"] or 0),
        num_turns=int(row["num_turns"] or 0),
        duration_ms=int(row["duration_ms"] or 0),
        total_cost_usd=round(float(row["total_cost_usd"] or 0.0), 12),
        input_tokens=int(row["input_tokens"] or 0),
        output_tokens=int(row["output_tokens"] or 0),
        cache_creation_input_tokens=int(row["cache_creation_input_tokens"] or 0),
        cache_read_input_tokens=int(row["cache_read_input_tokens"] or 0),
        total_tokens=int(row["total_tokens"] or 0),
    )


def _execution_from_row(row) -> Execution:
    return Execution(
        execution_id=row["execution_id"],
        session_id=row["session_id"],
        worker_address=row["worker_address"],
        started_at=_dt(row["started_at"]),
        halted_at=_dt(row["halted_at"]),
        halt_reason=row["halt_reason"],
        phase=ExecutionPhase(row["phase"] or ExecutionPhase.STARTING.value),
        runtime_container=ExecutionContainer.from_dict(
            json.loads(row["runtime_container_json"]) if row["runtime_container_json"] else None
        ),
        worker_container=ExecutionContainer.from_dict(
            json.loads(row["worker_container_json"]) if row["worker_container_json"] else None
        ),
        exit_code=row["exit_code"],
        claude_session_id=row["claude_session_id"],
        claude_num_turns=row["claude_num_turns"],
        claude_duration_ms=row["claude_duration_ms"],
        claude_total_cost_usd=row["claude_total_cost_usd"],
        claude_input_tokens=row["claude_input_tokens"],
        claude_output_tokens=row["claude_output_tokens"],
        claude_cache_creation_input_tokens=row["claude_cache_creation_input_tokens"],
        claude_cache_read_input_tokens=row["claude_cache_read_input_tokens"],
        claude_total_tokens=row["claude_total_tokens"],
        agent_error_category=row["agent_error_category"],
        agent_error_reason=row["agent_error_reason"],
    )


class Database:
    def __init__(self, path: str | Path):
        self._path = str(path)
        self._conn: aiosqlite.Connection | None = None

    async def init(self):
        self._conn = await aiosqlite.connect(self._path, timeout=30)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.executescript(_CREATE_TABLES)
        await self._conn.commit()
        for migration in [
            "CREATE TABLE IF NOT EXISTS attachments ("
            "attachment_id TEXT PRIMARY KEY, "
            "message_id TEXT NOT NULL REFERENCES messages(message_id), "
            "filename TEXT NOT NULL, content_type TEXT NOT NULL, storage_key TEXT NOT NULL);",
            "ALTER TABLE messages ADD COLUMN attachments TEXT NOT NULL DEFAULT '[]'",
            "ALTER TABLE messages ADD COLUMN sender TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE messages ADD COLUMN acknowledged_at TEXT",
            "ALTER TABLE messages ADD COLUMN delivery_error TEXT",
            "CREATE TABLE IF NOT EXISTS execution_logs ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "execution_id TEXT NOT NULL REFERENCES executions(execution_id), "
            "logged_at TEXT NOT NULL, "
            "stream TEXT NOT NULL CHECK(stream IN ('stdout', 'stderr')), "
            "body TEXT NOT NULL);",
            "CREATE TABLE IF NOT EXISTS proxy_logs ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "execution_id TEXT NOT NULL REFERENCES executions(execution_id), "
            "logged_at TEXT NOT NULL, "
            "stage TEXT NOT NULL, "
            "method TEXT, "
            "path TEXT, "
            "upstream_url TEXT, "
            "status_code INTEGER, "
            "content_type TEXT, "
            "body TEXT NOT NULL, "
            "meta TEXT NOT NULL DEFAULT '{}', "
            "input_tokens INTEGER, "
            "output_tokens INTEGER, "
            "cache_creation_input_tokens INTEGER, "
            "cache_read_input_tokens INTEGER, "
            "total_tokens INTEGER, "
            "total_cost_usd REAL);",
            "ALTER TABLE proxy_logs ADD COLUMN input_tokens INTEGER",
            "ALTER TABLE proxy_logs ADD COLUMN output_tokens INTEGER",
            "ALTER TABLE proxy_logs ADD COLUMN cache_creation_input_tokens INTEGER",
            "ALTER TABLE proxy_logs ADD COLUMN cache_read_input_tokens INTEGER",
            "ALTER TABLE proxy_logs ADD COLUMN total_tokens INTEGER",
            "ALTER TABLE proxy_logs ADD COLUMN total_cost_usd REAL",
            "ALTER TABLE executions ADD COLUMN exit_code INTEGER",
            "ALTER TABLE executions ADD COLUMN phase TEXT NOT NULL DEFAULT 'starting'",
            "ALTER TABLE executions ADD COLUMN runtime_container_json TEXT",
            "ALTER TABLE executions ADD COLUMN worker_container_json TEXT",
            "ALTER TABLE executions ADD COLUMN claude_session_id TEXT",
            "ALTER TABLE executions ADD COLUMN claude_num_turns INTEGER",
            "ALTER TABLE executions ADD COLUMN claude_duration_ms INTEGER",
            "ALTER TABLE executions ADD COLUMN claude_total_cost_usd REAL",
            "ALTER TABLE executions ADD COLUMN claude_input_tokens INTEGER",
            "ALTER TABLE executions ADD COLUMN claude_output_tokens INTEGER",
            "ALTER TABLE executions ADD COLUMN claude_cache_creation_input_tokens INTEGER",
            "ALTER TABLE executions ADD COLUMN claude_cache_read_input_tokens INTEGER",
            "ALTER TABLE executions ADD COLUMN claude_total_tokens INTEGER",
            "ALTER TABLE sessions ADD COLUMN last_execution_result TEXT",
            "ALTER TABLE sessions ADD COLUMN channel_metadata_json TEXT NOT NULL DEFAULT '{}'",
            "ALTER TABLE executions ADD COLUMN agent_error_category TEXT",
            "ALTER TABLE executions ADD COLUMN agent_error_reason TEXT",
            "ALTER TABLE proxy_logs ADD COLUMN request_id TEXT",
            "CREATE TABLE IF NOT EXISTS conversation_events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "execution_id TEXT NOT NULL REFERENCES executions(execution_id), "
            "logged_at TEXT NOT NULL, "
            "seq INTEGER NOT NULL, "
            "source TEXT NOT NULL CHECK(source IN ('local', 'claude')), "
            "event_type TEXT NOT NULL, "
            "event_subtype TEXT, "
            "payload_json TEXT NOT NULL);",
        ]:
            try:
                if migration.startswith("CREATE"):
                    await self._conn.executescript(migration)
                else:
                    await self._conn.execute(migration)
                await self._conn.commit()
            except Exception:
                await self._conn.rollback()

    async def close(self):
        if self._conn:
            await self._conn.close()

    async def create_session(self, session: Session) -> None:
        await self._conn.execute(
            "INSERT INTO sessions "
            "(session_id, thread_id, channel, workflow_name, state, workspace_path, "
            "created_at, last_message_at, last_execution_result, channel_metadata_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (session.session_id, session.thread_id, session.channel,
             session.workflow_name, session.state.value, session.workspace_path,
             session.created_at.isoformat(), session.last_message_at.isoformat(),
             session.last_execution_result, json.dumps(session.channel_metadata)),
        )
        await self._conn.commit()

    async def get_session_claude_summary(self, session_id: str) -> ClaudeSummary:
        cur = await self._conn.execute(
            "SELECT "
            "COUNT(*) AS executions_count, "
            "SUM(CASE WHEN halted_at IS NOT NULL THEN 1 ELSE 0 END) AS completed_executions_count, "
            "SUM(COALESCE(claude_num_turns, 0)) AS num_turns, "
            "SUM(COALESCE(claude_duration_ms, 0)) AS duration_ms, "
            "SUM(COALESCE(claude_total_cost_usd, 0)) AS total_cost_usd, "
            "SUM(COALESCE(claude_input_tokens, 0)) AS input_tokens, "
            "SUM(COALESCE(claude_output_tokens, 0)) AS output_tokens, "
            "SUM(COALESCE(claude_cache_creation_input_tokens, 0)) AS cache_creation_input_tokens, "
            "SUM(COALESCE(claude_cache_read_input_tokens, 0)) AS cache_read_input_tokens, "
            "SUM(COALESCE(claude_total_tokens, 0)) AS total_tokens "
            "FROM executions WHERE session_id = ?",
            (session_id,),
        )
        row = await cur.fetchone()
        return _summary_from_row(row)

    async def get_session_by_thread(self, thread_id: str) -> Session | None:
        cur = await self._conn.execute(
            "SELECT * FROM sessions WHERE thread_id = ?", (thread_id,)
        )
        row = await cur.fetchone()
        if not row:
            return None
        summary = await self.get_session_claude_summary(row["session_id"])
        return Session(
            session_id=row["session_id"], thread_id=row["thread_id"],
            channel=row["channel"], workflow_name=row["workflow_name"],
            state=SessionState(row["state"]), workspace_path=row["workspace_path"],
            created_at=_dt(row["created_at"]), last_message_at=_dt(row["last_message_at"]),
            last_execution_result=row["last_execution_result"],
            claude_summary=summary,
            channel_metadata=await self._get_effective_channel_metadata(row),
        )

    async def get_session(self, session_id: str) -> Session | None:
        cur = await self._conn.execute(
            "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
        )
        row = await cur.fetchone()
        if not row:
            return None
        summary = await self.get_session_claude_summary(row["session_id"])
        return Session(
            session_id=row["session_id"], thread_id=row["thread_id"],
            channel=row["channel"], workflow_name=row["workflow_name"],
            state=SessionState(row["state"]), workspace_path=row["workspace_path"],
            created_at=_dt(row["created_at"]), last_message_at=_dt(row["last_message_at"]),
            last_execution_result=row["last_execution_result"],
            claude_summary=summary,
            channel_metadata=await self._get_effective_channel_metadata(row),
        )

    async def update_session_state(self, session_id: str, state: SessionState) -> None:
        await self._conn.execute(
            "UPDATE sessions SET state = ?, last_message_at = ? WHERE session_id = ?",
            (state.value, datetime.now(timezone.utc).isoformat(), session_id),
        )
        await self._conn.commit()

    async def update_session_last_message_at(
        self, session_id: str, received_at: str | None = None
    ) -> None:
        await self._conn.execute(
            "UPDATE sessions SET last_message_at = ? WHERE session_id = ?",
            (received_at or datetime.now(timezone.utc).isoformat(), session_id),
        )
        await self._conn.commit()

    async def update_session_channel_metadata(self, session_id: str, metadata: dict) -> None:
        await self._conn.execute(
            "UPDATE sessions SET channel_metadata_json = ? WHERE session_id = ?",
            (json.dumps(metadata), session_id),
        )
        await self._conn.commit()

    async def list_sessions(self) -> list[Session]:
        cur = await self._conn.execute("SELECT * FROM sessions ORDER BY created_at DESC")
        rows = await cur.fetchall()
        sessions = []
        for r in rows:
            sessions.append(Session(
                session_id=r["session_id"], thread_id=r["thread_id"],
                channel=r["channel"], workflow_name=r["workflow_name"],
                state=SessionState(r["state"]), workspace_path=r["workspace_path"],
                created_at=_dt(r["created_at"]), last_message_at=_dt(r["last_message_at"]),
                last_execution_result=r["last_execution_result"],
                claude_summary=await self.get_session_claude_summary(r["session_id"]),
                channel_metadata=await self._get_effective_channel_metadata(r),
            ))
        return sessions

    async def _get_effective_channel_metadata(self, row) -> dict:
        metadata = self._parse_channel_metadata(row["channel_metadata_json"])
        if not metadata.get("sender"):
            cur = await self._conn.execute(
                "SELECT sender FROM messages "
                "WHERE session_id = ? AND direction = 'inbound' AND sender != '' "
                "ORDER BY received_at LIMIT 1",
                (row["session_id"],),
            )
            sender_row = await cur.fetchone()
            if sender_row and sender_row["sender"]:
                metadata["sender"] = sender_row["sender"]
        return metadata

    def _parse_channel_metadata(self, raw: str | None) -> dict:
        try:
            metadata = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            metadata = {}
        return metadata if isinstance(metadata, dict) else {}

    async def create_execution(self, execution: Execution) -> None:
        await self._conn.execute(
            "INSERT INTO executions "
            "(execution_id, session_id, worker_address, started_at, halted_at, halt_reason, phase, "
            "runtime_container_json, worker_container_json, exit_code, "
            "claude_session_id, claude_num_turns, claude_duration_ms, claude_total_cost_usd, "
            "claude_input_tokens, claude_output_tokens, claude_cache_creation_input_tokens, "
            "claude_cache_read_input_tokens, claude_total_tokens) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (execution.execution_id, execution.session_id, execution.worker_address,
             execution.started_at.isoformat(),
             execution.halted_at.isoformat() if execution.halted_at else None,
             execution.halt_reason, execution.phase.value,
             json.dumps(execution.runtime_container.to_dict()) if execution.runtime_container else None,
             json.dumps(execution.worker_container.to_dict()) if execution.worker_container else None,
             execution.exit_code, execution.claude_session_id,
             execution.claude_num_turns, execution.claude_duration_ms, execution.claude_total_cost_usd,
             execution.claude_input_tokens, execution.claude_output_tokens,
             execution.claude_cache_creation_input_tokens, execution.claude_cache_read_input_tokens,
             execution.claude_total_tokens),
        )
        await self._conn.commit()

    async def get_active_execution(self, session_id: str) -> Execution | None:
        cur = await self._conn.execute(
            "SELECT * FROM executions WHERE session_id = ? AND halted_at IS NULL "
            "ORDER BY started_at DESC LIMIT 1",
            (session_id,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        return _execution_from_row(row)

    async def update_execution_claude_session_id(
        self, execution_id: str, claude_session_id: str
    ) -> None:
        await self._conn.execute(
            "UPDATE executions SET claude_session_id = ? WHERE execution_id = ?",
            (claude_session_id, execution_id),
        )
        await self._conn.commit()

    async def update_execution_worker_address(
        self, execution_id: str, worker_address: str | None
    ) -> None:
        await self._conn.execute(
            "UPDATE executions SET worker_address = ? WHERE execution_id = ?",
            (worker_address, execution_id),
        )
        await self._conn.commit()

    async def update_execution_status(
        self,
        execution_id: str,
        *,
        phase: ExecutionPhase | None = None,
        runtime_container: ExecutionContainer | None = None,
        worker_container: ExecutionContainer | None = None,
    ) -> None:
        updates: list[str] = []
        values: list[object] = []
        if phase is not None:
            updates.append("phase = ?")
            values.append(phase.value)
        if runtime_container is not None:
            updates.append("runtime_container_json = ?")
            values.append(json.dumps(runtime_container.to_dict()))
        if worker_container is not None:
            updates.append("worker_container_json = ?")
            values.append(json.dumps(worker_container.to_dict()))
        if not updates:
            return
        values.append(execution_id)
        await self._conn.execute(
            f"UPDATE executions SET {', '.join(updates)} WHERE execution_id = ?",
            values,
        )
        await self._conn.commit()

    async def update_execution_claude_result(
        self,
        execution_id: str,
        *,
        num_turns: int | None = None,
        duration_ms: int | None = None,
    ) -> None:
        await self._conn.execute(
            "UPDATE executions SET "
            "claude_num_turns = COALESCE(?, claude_num_turns), "
            "claude_duration_ms = COALESCE(?, claude_duration_ms) "
            "WHERE execution_id = ?",
            (num_turns, duration_ms, execution_id),
        )
        await self._conn.commit()

    async def update_execution_agent_error(
        self, execution_id: str, category: str, reason: str
    ) -> None:
        await self._conn.execute(
            "UPDATE executions SET agent_error_category = ?, agent_error_reason = ? "
            "WHERE execution_id = ?",
            (category, reason, execution_id),
        )
        await self._conn.commit()

    async def increment_execution_token_usage(
        self,
        execution_id: str,
        *,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cache_creation_input_tokens: int = 0,
        cache_read_input_tokens: int = 0,
        total_cost_usd: float | None = None,
    ) -> None:
        total_tokens = (
            int(input_tokens)
            + int(output_tokens)
            + int(cache_creation_input_tokens)
            + int(cache_read_input_tokens)
        )
        await self._conn.execute(
            "UPDATE executions SET "
            "claude_input_tokens = COALESCE(claude_input_tokens, 0) + ?, "
            "claude_output_tokens = COALESCE(claude_output_tokens, 0) + ?, "
            "claude_cache_creation_input_tokens = COALESCE(claude_cache_creation_input_tokens, 0) + ?, "
            "claude_cache_read_input_tokens = COALESCE(claude_cache_read_input_tokens, 0) + ?, "
            "claude_total_tokens = COALESCE(claude_total_tokens, 0) + ?, "
            "claude_total_cost_usd = COALESCE(claude_total_cost_usd, 0) + COALESCE(?, 0) "
            "WHERE execution_id = ?",
            (
                int(input_tokens),
                int(output_tokens),
                int(cache_creation_input_tokens),
                int(cache_read_input_tokens),
                total_tokens,
                float(total_cost_usd) if total_cost_usd is not None else None,
                execution_id,
            ),
        )
        await self._conn.commit()

    async def get_latest_execution(self, session_id: str) -> Execution | None:
        cur = await self._conn.execute(
            "SELECT * FROM executions WHERE session_id = ? ORDER BY started_at DESC LIMIT 1",
            (session_id,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        return _execution_from_row(row)

    async def get_execution(self, execution_id: str) -> Execution | None:
        cur = await self._conn.execute(
            "SELECT * FROM executions WHERE execution_id = ?",
            (execution_id,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        return _execution_from_row(row)

    async def halt_execution(
        self, execution_id: str, reason: str, exit_code: int | None = None
    ) -> None:
        phase = ExecutionPhase.FINISHED if reason == "exited" and exit_code == 0 else ExecutionPhase.FAILED
        await self._conn.execute(
            "UPDATE executions SET halted_at = ?, halt_reason = ?, exit_code = ?, phase = ? "
            "WHERE execution_id = ?",
            (datetime.now(timezone.utc).isoformat(), reason, exit_code, phase.value, execution_id),
        )
        await self._conn.commit()

    async def update_session_last_execution_result(
        self, session_id: str, result: str
    ) -> None:
        await self._conn.execute(
            "UPDATE sessions SET last_execution_result = ? WHERE session_id = ?",
            (result, session_id),
        )
        await self._conn.commit()

    async def append_execution_log(self, execution_id: str, line: LogLine) -> None:
        await self._conn.execute(
            "INSERT INTO execution_logs (execution_id, logged_at, stream, body) VALUES (?,?,?,?)",
            (execution_id, line.logged_at.isoformat(), line.stream, line.body),
        )
        await self._conn.commit()

    async def list_execution_logs(self, execution_id: str) -> list[dict]:
        cur = await self._conn.execute(
            "SELECT logged_at, stream, body FROM execution_logs "
            "WHERE execution_id = ? ORDER BY logged_at",
            (execution_id,),
        )
        rows = await cur.fetchall()
        return [
            {"logged_at": r["logged_at"], "stream": r["stream"], "body": r["body"]}
            for r in rows
        ]

    async def append_proxy_log(
        self,
        execution_id: str,
        *,
        stage: str,
        body: str,
        method: str | None = None,
        path: str | None = None,
        upstream_url: str | None = None,
        status_code: int | None = None,
        content_type: str | None = None,
        meta: dict | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        cache_creation_input_tokens: int | None = None,
        cache_read_input_tokens: int | None = None,
        total_tokens: int | None = None,
        total_cost_usd: float | None = None,
        logged_at: datetime | None = None,
        request_id: str | None = None,
    ) -> None:
        await self._conn.execute(
            "INSERT INTO proxy_logs "
            "(execution_id, logged_at, stage, method, path, upstream_url, status_code, content_type, body, meta, "
            "input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens, total_tokens, total_cost_usd, request_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                execution_id,
                (logged_at or datetime.now(timezone.utc)).isoformat(),
                stage,
                method,
                path,
                upstream_url,
                status_code,
                content_type,
                body,
                json.dumps(meta or {}),
                input_tokens,
                output_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                total_tokens,
                float(total_cost_usd) if total_cost_usd is not None else None,
                request_id,
            ),
        )
        await self._conn.commit()

    async def list_proxy_logs(self, execution_id: str) -> list[dict]:
        cur = await self._conn.execute(
            "SELECT logged_at, stage, method, path, upstream_url, status_code, content_type, body, meta, "
            "input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens, total_tokens, total_cost_usd, request_id "
            "FROM proxy_logs WHERE execution_id = ? ORDER BY logged_at, id",
            (execution_id,),
        )
        rows = await cur.fetchall()
        return [
            {
                "logged_at": r["logged_at"],
                "stage": r["stage"],
                "method": r["method"],
                "path": r["path"],
                "upstream_url": r["upstream_url"],
                "status_code": r["status_code"],
                "content_type": r["content_type"],
                "body": r["body"],
                "meta": json.loads(r["meta"]),
                "input_tokens": r["input_tokens"],
                "output_tokens": r["output_tokens"],
                "cache_creation_input_tokens": r["cache_creation_input_tokens"],
                "cache_read_input_tokens": r["cache_read_input_tokens"],
                "total_tokens": r["total_tokens"],
                "total_cost_usd": r["total_cost_usd"],
                "request_id": r["request_id"],
            }
            for r in rows
        ]

    async def list_executions(self, session_id: str) -> list[Execution]:
        cur = await self._conn.execute(
            "SELECT * FROM executions WHERE session_id = ? ORDER BY started_at",
            (session_id,),
        )
        rows = await cur.fetchall()
        return [_execution_from_row(r) for r in rows]

    async def list_active_executions(self) -> list[Execution]:
        cur = await self._conn.execute(
            "SELECT * FROM executions WHERE halted_at IS NULL ORDER BY started_at"
        )
        rows = await cur.fetchall()
        return [_execution_from_row(r) for r in rows]

    async def get_last_log_timestamp(self, execution_id: str) -> datetime | None:
        cur = await self._conn.execute(
            "SELECT MAX(logged_at) AS ts FROM execution_logs WHERE execution_id = ?",
            (execution_id,),
        )
        row = await cur.fetchone()
        if row is None or row["ts"] is None:
            return None
        return _dt(row["ts"])

    async def store_message(
        self,
        session_id: str,
        direction: str,
        body: str,
        attachments: list[dict] | None = None,
        sender: str = "",
    ) -> str:
        message_id = str(uuid.uuid4())
        await self._conn.execute(
            "INSERT INTO messages "
            "(message_id, session_id, direction, body, received_at, attachments, sender, "
            "acknowledged_at, delivery_error) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (message_id, session_id, direction, body,
             datetime.now(timezone.utc).isoformat(),
             json.dumps(attachments or []), sender, None, None),
        )
        await self._conn.commit()
        return message_id

    async def get_next_conversation_event_seq(self, execution_id: str) -> int:
        cur = await self._conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS max_seq FROM conversation_events "
            "WHERE execution_id = ?",
            (execution_id,),
        )
        row = await cur.fetchone()
        return int(row["max_seq"]) + 1

    async def append_conversation_event(
        self,
        execution_id: str,
        *,
        source: str,
        event_type: str,
        payload: dict,
        event_subtype: str | None = None,
        logged_at: datetime | None = None,
        seq: int | None = None,
    ) -> dict:
        event_seq = seq if seq is not None else await self.get_next_conversation_event_seq(execution_id)
        event_time = logged_at or datetime.now(timezone.utc)
        await self._conn.execute(
            "INSERT INTO conversation_events "
            "(execution_id, logged_at, seq, source, event_type, event_subtype, payload_json) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                execution_id,
                event_time.isoformat(),
                event_seq,
                source,
                event_type,
                event_subtype,
                json.dumps(payload),
            ),
        )
        await self._conn.commit()
        return {
            "execution_id": execution_id,
            "logged_at": event_time.isoformat(),
            "seq": event_seq,
            "source": source,
            "event_type": event_type,
            "event_subtype": event_subtype,
            "payload": payload,
        }

    async def list_conversation_events(self, execution_id: str) -> list[dict]:
        cur = await self._conn.execute(
            "SELECT logged_at, seq, source, event_type, event_subtype, payload_json "
            "FROM conversation_events WHERE execution_id = ? ORDER BY seq, id",
            (execution_id,),
        )
        rows = await cur.fetchall()
        return [
            {
                "execution_id": execution_id,
                "logged_at": r["logged_at"],
                "seq": r["seq"],
                "source": r["source"],
                "event_type": r["event_type"],
                "event_subtype": r["event_subtype"],
                "payload": json.loads(r["payload_json"]),
            }
            for r in rows
        ]

    async def acknowledge_message(self, message_id: str) -> str:
        acknowledged_at = datetime.now(timezone.utc).isoformat()
        await self._conn.execute(
            "UPDATE messages SET acknowledged_at = ?, delivery_error = NULL WHERE message_id = ?",
            (acknowledged_at, message_id),
        )
        await self._conn.commit()
        return acknowledged_at

    async def acknowledge_messages(self, message_ids: list[str]) -> str:
        if not message_ids:
            return datetime.now(timezone.utc).isoformat()
        placeholders = ",".join("?" * len(message_ids))
        now = datetime.now(timezone.utc).isoformat()
        await self._conn.execute(
            f"UPDATE messages SET acknowledged_at = ?, delivery_error = NULL "
            f"WHERE message_id IN ({placeholders})",
            [now, *message_ids],
        )
        await self._conn.commit()
        return now

    async def fail_message_delivery(self, message_id: str, error: str) -> str:
        failed_at = datetime.now(timezone.utc).isoformat()
        await self._conn.execute(
            "UPDATE messages "
            "SET acknowledged_at = COALESCE(acknowledged_at, ?), delivery_error = ? "
            "WHERE message_id = ?",
            (failed_at, error, message_id),
        )
        await self._conn.commit()
        return failed_at

    async def fail_message_deliveries(self, message_ids: list[str], error: str) -> str:
        if not message_ids:
            return datetime.now(timezone.utc).isoformat()
        failed_at = datetime.now(timezone.utc).isoformat()
        placeholders = ",".join("?" * len(message_ids))
        await self._conn.execute(
            f"UPDATE messages "
            f"SET acknowledged_at = COALESCE(acknowledged_at, ?), delivery_error = ? "
            f"WHERE message_id IN ({placeholders})",
            [failed_at, error, *message_ids],
        )
        await self._conn.commit()
        return failed_at

    async def get_pending_inbound_messages(self, session_id: str) -> list[dict]:
        cur = await self._conn.execute(
            "SELECT message_id, body, sender, received_at, attachments FROM messages "
            "WHERE session_id = ? AND direction = 'inbound' AND acknowledged_at IS NULL "
            "ORDER BY received_at",
            (session_id,),
        )
        rows = await cur.fetchall()
        return [
            {
                "message_id": r["message_id"],
                "direction": "inbound",
                "body": r["body"],
                "sender": r["sender"],
                "received_at": r["received_at"],
                "attachments": json.loads(r["attachments"]),
            }
            for r in rows
        ]

    async def delete_pending_message(self, session_id: str, message_id: str) -> dict | None:
        async with self._conn.execute(
            "SELECT message_id, body, sender, received_at, attachments "
            "FROM messages "
            "WHERE session_id = ? AND message_id = ? "
            "AND direction = 'inbound' AND acknowledged_at IS NULL",
            (session_id, message_id),
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return None

        async with self._conn.execute(
            "SELECT storage_key FROM attachments WHERE message_id = ?",
            (message_id,),
        ) as cur:
            attachment_rows = await cur.fetchall()
        storage_keys = [r["storage_key"] for r in attachment_rows]

        await self._conn.execute(
            "DELETE FROM attachments WHERE message_id = ?",
            (message_id,),
        )
        await self._conn.execute(
            "DELETE FROM messages WHERE session_id = ? AND message_id = ?",
            (session_id, message_id),
        )

        async with self._conn.execute(
            "SELECT COALESCE(MAX(m.received_at), s.created_at) AS last_message_at "
            "FROM sessions s "
            "LEFT JOIN messages m ON m.session_id = s.session_id "
            "WHERE s.session_id = ?",
            (session_id,),
        ) as cur:
            last_row = await cur.fetchone()
        await self._conn.execute(
            "UPDATE sessions SET last_message_at = ? WHERE session_id = ?",
            (last_row["last_message_at"], session_id),
        )
        await self._conn.commit()

        return {
            "message_id": row["message_id"],
            "direction": "inbound",
            "body": row["body"],
            "sender": row["sender"],
            "received_at": row["received_at"],
            "attachments": json.loads(row["attachments"]),
            "storage_keys": storage_keys,
        }

    async def store_attachment(
        self, message_id: str, filename: str, content_type: str, storage_key: str
    ) -> str:
        attachment_id = str(uuid.uuid4())
        await self._conn.execute(
            "INSERT INTO attachments VALUES (?,?,?,?,?)",
            (attachment_id, message_id, filename, content_type, storage_key),
        )
        await self._conn.commit()
        return attachment_id

    async def get_attachment_by_name(
        self, message_id: str, filename: str
    ) -> dict | None:
        cur = await self._conn.execute(
            "SELECT storage_key, content_type FROM attachments "
            "WHERE message_id = ? AND filename = ?",
            (message_id, filename),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return {"storage_key": row["storage_key"], "content_type": row["content_type"]}

    async def get_conversation(self, session_id: str) -> list[dict]:
        """Return messages in {role, content, attachments} form for the conversation endpoint."""
        async with self._conn.execute(
            "SELECT m.message_id, m.direction, m.body, "
            "a.filename, a.content_type "
            "FROM messages m "
            "LEFT JOIN attachments a ON a.message_id = m.message_id "
            "WHERE m.session_id = ? AND m.direction IN ('inbound', 'outbound') "
            "ORDER BY m.received_at, a.rowid",
            (session_id,),
        ) as cur:
            rows = await cur.fetchall()

        # Collapse the JOIN rows back into per-message dicts.
        seen: dict[str, dict] = {}
        order: list[str] = []
        for row in rows:
            mid = row["message_id"]
            if mid not in seen:
                seen[mid] = {
                    "role": "user" if row["direction"] == "inbound" else "assistant",
                    "content": row["body"],
                    "attachments": [],
                }
                order.append(mid)
            if row["filename"]:
                seen[mid]["attachments"].append({
                    "message_id": mid,
                    "filename": row["filename"],
                    "content_type": row["content_type"],
                })
        return [seen[mid] for mid in order]

    async def get_session_history(
        self,
        session_id: str,
        *,
        side: str = "both",
        limit: int = 20,
        query: str = "",
    ) -> list[dict]:
        direction_map = {
            "user": ("inbound",),
            "assistant": ("outbound",),
            "both": ("inbound", "outbound"),
        }
        directions = direction_map.get(side, direction_map["both"])
        capped_limit = max(1, min(limit, 100))
        pattern = f"%{query.lower()}%" if query else None
        placeholders = ",".join("?" for _ in directions)

        sql = (
            "SELECT message_id, direction, body, received_at, attachments "
            "FROM ("
            " SELECT message_id, direction, body, received_at, attachments "
            " FROM messages "
            f" WHERE session_id = ? AND direction IN ({placeholders})"
        )
        params: list[object] = [session_id, *directions]
        if pattern is not None:
            sql += " AND lower(body) LIKE ?"
            params.append(pattern)
        sql += " ORDER BY received_at DESC LIMIT ?"
        params.append(capped_limit)
        sql += ") ORDER BY received_at ASC"

        async with self._conn.execute(sql, params) as cur:
            rows = await cur.fetchall()
        return [
            {
                "message_id": r["message_id"],
                "role": "user" if r["direction"] == "inbound" else "assistant",
                "content": r["body"],
                "received_at": r["received_at"],
                "attachments": json.loads(r["attachments"]),
            }
            for r in rows
        ]

    async def is_execution_active(self, session_id: str) -> bool:
        async with self._conn.execute(
            "SELECT 1 FROM executions WHERE session_id = ? AND halted_at IS NULL LIMIT 1",
            (session_id,),
        ) as cur:
            return await cur.fetchone() is not None

    async def list_messages(self, session_id: str) -> list[dict]:
        cur = await self._conn.execute(
            "SELECT message_id, direction, body, sender, received_at, attachments, acknowledged_at, delivery_error "
            "FROM messages "
            "WHERE session_id = ? ORDER BY received_at",
            (session_id,),
        )
        rows = await cur.fetchall()
        return [
            {
                "message_id": r["message_id"],
                "direction": r["direction"],
                "body": r["body"],
                "sender": r["sender"],
                "received_at": r["received_at"],
                "attachments": json.loads(r["attachments"]),
                "acknowledged_at": r["acknowledged_at"],
                "delivery_error": r["delivery_error"],
                "delivery_status": (
                    "failed"
                    if r["direction"] == "inbound" and r["delivery_error"]
                    else (
                        "pending"
                        if r["direction"] == "inbound" and r["acknowledged_at"] is None
                        else "acknowledged"
                    )
                ),
            }
            for r in rows
        ]

    async def get_message(self, session_id: str, message_id: str) -> dict | None:
        cur = await self._conn.execute(
            "SELECT message_id, direction, body, sender, received_at, attachments, acknowledged_at, delivery_error "
            "FROM messages "
            "WHERE session_id = ? AND message_id = ?",
            (session_id, message_id),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return {
            "message_id": row["message_id"],
            "direction": row["direction"],
            "body": row["body"],
            "sender": row["sender"],
            "received_at": row["received_at"],
            "attachments": json.loads(row["attachments"]),
            "acknowledged_at": row["acknowledged_at"],
            "delivery_error": row["delivery_error"],
            "delivery_status": (
                "failed"
                if row["direction"] == "inbound" and row["delivery_error"]
                else (
                    "pending"
                    if row["direction"] == "inbound" and row["acknowledged_at"] is None
                    else "acknowledged"
                )
            ),
        }
