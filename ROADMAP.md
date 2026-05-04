# Claudius Roadmap

This document tracks the work needed to bring Claudius up to its full architectural vision. The platform today has a solid foundation: email ingestion, Docker-based execution, session state management, SQLite persistence, a proxy layer for upstream LLMs, and a basic React frontend. The roadmap below closes the gap between that foundation and the full design.

---

## Phase 1 — Core Domain Model

The current data model conflates concepts that the design keeps separate: a **Task** (what the user asked for), a **TaskAttempt** (one execution of that task), and a **TaskOutput** (an artifact the task produced). Executions today mix all three. This phase establishes the canonical entities.

### 1.1 Task / TaskAttempt split

- Introduce a `Task` entity with its own state machine: `PENDING → RUNNING → AWAITING_REPLY ⇄ RUNNING → COMPLETED | FAILED | CANCELLED`
- Rename the current `Execution` concept to `TaskAttempt`, which tracks one concrete run of a Task on a sandbox
- A Task may have multiple TaskAttempts (retry, resume after failure)
- Wire `POST /sessions/{id}/tasks` as the primary task submission endpoint; `POST /sessions/{id}/message` becomes a thin wrapper

### 1.2 TaskOutput / Deliverables

- Add a `TaskOutput` entity: a heterogeneous artifact a Task produces — a merge request, a file, a generated document, or a generic URL
- Link TaskOutputs to the TaskAttempt that produced them (so retried tasks distinguish first and second outputs)
- Expose `GET /tasks/{id}` returning status, cost, token usage, and linked TaskOutputs
- Add a **Deliverables panel** to the frontend showing all outputs across a session's tasks

### 1.3 AgentEvent discriminated union

- Harden the conversation event model into a typed discriminated union with these variants: `assistant_message`, `user_message`, `system_message`, `result_message`, `tool_call`, `file_edit`, `status_change`, `clarifying_question`
- `clarifying_question` is the only variant that blocks a Task (triggers `AWAITING_REPLY`)
- Migrate the existing `conversation_events` table to this schema

---

## Phase 2 — Clarifying Questions

The design requires the agent to be able to pause mid-task and ask the user a question, resuming only after a reply.

- Implement the `AWAITING_REPLY` state on Task (and propagate up to Session as `needs_input`)
- When the agent emits a `clarifying_question` AgentEvent, transition the Task to `AWAITING_REPLY`
- Add `POST /tasks/{id}/reply` to accept the user's answer and return the Task to `RUNNING`
- Surface clarifying questions prominently in the conversation UI with a dedicated reply input

---

## Phase 3 — Persistence & Coordination Layer

SQLite and the in-process SSE broker are adequate for a single-process deployment; they become bottlenecks at scale and make stateless orchestration impossible.

### 3.1 PostgreSQL

- Replace SQLite with PostgreSQL
- Use `SELECT … FOR UPDATE SKIP LOCKED` for idempotent task claim by the orchestrator
- Add a heartbeat column on TaskAttempt so dead orchestrator pods can be detected and their claims reclaimed

### 3.2 Redis

- Add Redis as the ephemeral coordination layer:
  - **Task dispatch**: Redis Streams consumer group replaces in-process polling
  - **SSE fan-out**: publish AgentEvents to a Redis channel; any API replica can serve any SSE client (eliminates sticky sessions)
  - **Rate limiting**: sliding-window counters per user per model
  - **Idempotency**: deduplication key store for `POST /sessions/{id}/tasks` (honors `Idempotency-Key` header)
  - **Token streaming**: live token relay from orchestrator to SSE subscribers

---

## Phase 4 — Stateless Orchestrator

The session manager today is a monolith that mixes routing, execution, state transitions, and container management. The design separates these into a stateless, horizontally-scalable orchestrator.

- Extract an **Orchestrator** process: consumes PENDING Tasks from Redis Streams, creates TaskAttempts, drives the agent loop, persists AgentEvents to Postgres, publishes events to Redis
- Orchestrator is stateless and idempotent: if a pod dies mid-attempt, another picks up the TaskAttempt via heartbeat timeout + DB lock
- The orchestrator resumes the Claude SDK session using `resume=session_id` so no conversation context is lost on failover
- Scale orchestrator replicas independently of the API gateway

---

## Phase 5 — Tool Catalog & Credential Management

Tools and credentials today are implicit in workflow YAML files. The design makes them first-class entities.

### 5.1 Tool Catalog

- Introduce `ToolDefinition`: a static catalog entry with `id`, `vendor`, `description`, and the credential kind it requires
- Initial catalog: `gitlab`, `confluence`, `ms_outlook`, `ms_calendar`, `web_search`, `code_sandbox`, `pdf_gen`
- Expose `GET /tools` to list available tool definitions

### 5.2 ToolBinding

- Introduce `ToolBinding`: a per-Session mapping of a ToolDefinition to a Credential with an optional scope
- `POST /sessions` accepts `{ tools[], credentials[] }` to bind tools at session creation
- The orchestrator assembles MCP tool servers based on the session's active ToolBindings

### 5.3 Credential Entity

- Promote credentials to a first-class entity: `POST /credentials` registers a credential (OAuth flow or token entry); the secret is stored in Vault, only metadata in Postgres
- `GET /credentials` lists the user's credentials with last-4 fingerprint and scope summary
- Credentials are never returned raw by any API endpoint

### 5.4 Per-task credential isolation

- At TaskAttempt start, mint a per-task scoped Vault role bound to a per-task Kubernetes ServiceAccount
- A Credential Proxy sidecar in the sandbox authenticates to Vault, materializes secrets to `tmpfs`, and injects auth into outbound tool calls
- At TaskAttempt end, revoke the Vault role and ServiceAccount; the tmpfs evaporates with the pod
- The agent process never holds a Vault token or raw secret

---

## Phase 6 — Kubernetes Backend & Sandbox Model

Docker is fine locally; Kubernetes is required for production isolation and horizontal scale.

### 6.1 Kubernetes execution backend

- Implement a Kubernetes backend alongside the existing Docker backend (selectable via config)
- Each TaskAttempt spawns a Kubernetes Job with the worker container, Credential Proxy sidecar, and MCP tool server sidecars
- NetworkPolicy derived from the Session's ToolBindings (egress allowed only to bound tool endpoints)

### 6.2 Workspace persistence

- Back the Workspace with S3-versioned bundles (already stubbed in the config schema)
- Workspace survives sandbox destruction and is remounted when a new sandbox is created for the same Session
- Implement the `HIBERNATED` session state fully: persist workspace to S3, destroy sandbox compute, resume from S3 on next Task

### 6.3 Sandbox warm pool (deferred)

- Adopt the Kubernetes Sandbox CRD (`SandboxTemplate`, `SandboxClaim`, `SandboxWarmPool`) when cold-start latency becomes measurable at scale
- Warm pool sized to expected concurrency; claims resolve in under 200 ms
- Pause/resume via memory snapshot to object storage for long-lived sessions between Tasks
- This is an additive migration: only the orchestrator's `create Job` call changes to `create SandboxClaim`

---

## Phase 7 — User Authentication & ACL

The platform currently has no user identity layer.

- Integrate OIDC/SSO for user authentication at the API Gateway
- Introduce a `User` entity (id, email, group memberships, clearance level)
- Enforce that Sessions and Credentials are scoped to the owning user; no cross-user access
- Propagate user identity downstream via request headers (not re-authenticated per service)
- Add per-user rate limiting (Phase 3.2 Redis counters keyed by user ID)

---

## Phase 8 — Channel Expansion

Email via Resend is the only channel today.

### 8.1 Slack channel

- Implement Slack as an ingress channel (OAuth app install, no per-user credential needed)
- Support two routing modes:
  - `thread`: each Slack thread becomes its own Session; replies append Tasks
  - `session`: all messages in a channel route to a single pre-bound Session
- Outbound: post agent responses back to the originating thread

### 8.2 Channel management API

- `POST /channels` — register a channel (email or Slack) with routing config
- `GET /channels` — list configured channels
- Move channel configuration from static YAML into the database

---

## Phase 9 — Frontend & UX Polish

### 9.1 Task-centric UI

- Replace the flat message timeline with a task-oriented view: each Task is a collapsible card showing prompt, status, cost, duration, and linked Deliverables
- Show task status transitions in real time via SSE

### 9.2 Deliverables panel

- Persistent panel in the session view listing all TaskOutputs across the session
- Each deliverable shows its type (MR, document, file, link), producing task, and a primary action (open, download)

### 9.3 Clarifying questions

- When a Task enters `AWAITING_REPLY`, surface the question inline in the conversation with a reply input
- Block further task submission until the question is answered or the task is cancelled

### 9.4 Session creation flow

- Replace the workflow-dropdown new-session modal with a tool-and-credential binding flow aligned with the tool catalog
- Show available tools, let the user bind credentials per tool, preview the effective scope before creating the session

---

## Phase 10 — Observability & Operations

- Structured JSON logging throughout (Loguru already present; standardize field names)
- Prometheus metrics: task throughput, attempt latency, token usage, sandbox start time, SSE subscriber count
- Audit log API: `GET /audit` with filtering by session, user, time range
- Alerting on stale tasks (running beyond expected wall-clock time) and orphaned sandboxes
- Health endpoints expanded to cover database, Redis, and Kubernetes connectivity

---

## Phase 11 — High-Side / Air-Gap Deployment

- Document and test deployment with no outbound internet access
- Replace Resend with a self-hosted SMTP relay option
- Vault/OpenBao deployment guide for secrets management
- Ensure all container images can be mirrored to a private registry with no pull-through
- Provide a `docker-compose` stack covering all dependencies (Postgres, Redis, Vault, LiteLLM) for standalone deployment
