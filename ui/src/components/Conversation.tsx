import { useEffect, useRef, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import {
  ApiError,
  getMessages,
  getSession,
  listExecutions,
  sendMessage,
  stopExecution,
  subscribeToSession,
} from '../api'
import type {
  Attachment,
  Execution,
  FollowupAction,
  Message,
  MessageAttachment,
  Session,
  SessionState,
  SSEEvent,
} from '../types'
import ConversationEventPanel from './ConversationEventPanel'
import LogPanel from './LogPanel'
import MarkdownMessage from './MarkdownMessage'
import ProxyLogPanel from './ProxyLogPanel'

function readAsBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => resolve((reader.result as string).split(',')[1])
    reader.onerror = reject
    reader.readAsDataURL(file)
  })
}

const STATE_COLOR: Record<string, string> = {
  active: 'text-green-400',
  waiting: 'text-orange-400',
  hibernated: 'text-gray-500',
  closed: 'text-gray-500',
  new: 'text-blue-400',
  error: 'text-red-400',
}

const USER_SAFE_ERROR_MESSAGE =
  "I couldn’t start the workspace for this request. Please try again in a moment."

type TimelineItem =
  | { key: string; kind: 'message'; ts: string; data: Message }
  | { key: string; kind: 'execution-start'; ts: string; data: Execution }
  | { key: string; kind: 'execution-end'; ts: string; data: Execution }

const TIMELINE_KIND_ORDER: Record<TimelineItem['kind'], number> = {
  'execution-start': 0,
  message: 1,
  'execution-end': 2,
}

function upsertMessage(messages: Message[], next: Message): Message[] {
  const idx = messages.findIndex((message) => message.message_id === next.message_id)
  if (idx >= 0) {
    const updated = [...messages]
    updated[idx] = { ...updated[idx], ...next }
    return updated
  }
  return [...messages, next]
}

function acknowledgeMessage(messages: Message[], messageId: string, acknowledgedAt: string): Message[] {
  return messages.map((message) =>
    message.message_id === messageId
      ? {
          ...message,
          acknowledged_at: acknowledgedAt,
          delivery_status: 'acknowledged',
          delivery_error: null,
        }
      : message,
  )
}

function buildTimeline(
  messages: Message[],
  executions: Execution[],
  executionActivityAt: Record<string, string>,
): TimelineItem[] {
  const items: TimelineItem[] = [
    ...messages.map((m) => ({
      key: `message-${m.message_id}`,
      kind: 'message' as const,
      ts: m.received_at,
      data: m,
    })),
    ...executions.map((e) => ({
      key: `execution-start-${e.execution_id}`,
      kind: 'execution-start' as const,
      ts: e.started_at,
      data: e,
    })),
    ...executions.map((e) => ({
      key: `execution-end-${e.execution_id}`,
      kind: 'execution-end' as const,
      ts: e.halted_at ?? executionActivityAt[e.execution_id] ?? e.started_at,
      data: e,
    })),
  ]
  items.sort((a, b) => {
    const byTimestamp = a.ts.localeCompare(b.ts)
    if (byTimestamp !== 0) return byTimestamp
    return TIMELINE_KIND_ORDER[a.kind] - TIMELINE_KIND_ORDER[b.kind]
  })
  return items
}

function formatDuration(durationMs: number): string | null {
  if (!durationMs) return null
  if (durationMs < 60_000) return `${(durationMs / 1000).toFixed(1)}s`
  return `${(durationMs / 60_000).toFixed(1)}m`
}

function formatLiveDuration(durationMs: number): string {
  if (durationMs < 60_000) return `${Math.max(durationMs, 0) / 1000 >= 10 ? Math.round(durationMs / 1000) : (durationMs / 1000).toFixed(1)}s`
  return `${(durationMs / 60_000).toFixed(1)}m`
}

function formatCost(totalCostUsd: number): string | null {
  return totalCostUsd ? `$${totalCostUsd.toFixed(4)}` : null
}

function formatHeartbeatAge(durationMs: number): string {
  if (durationMs < 2_000) return 'heartbeat just now'
  if (durationMs < 60_000) return `heartbeat ${Math.round(durationMs / 1000)}s ago`
  return `heartbeat ${(durationMs / 60_000).toFixed(1)}m ago`
}

function buildExecutionSummary(execution: Execution): string {
  const parts = [
    execution.claude_num_turns ? `${execution.claude_num_turns} turns` : null,
    execution.claude_duration_ms ? formatDuration(execution.claude_duration_ms) : null,
    execution.claude_total_cost_usd ? formatCost(execution.claude_total_cost_usd) : null,
  ].filter(Boolean)
  return parts.join(' · ')
}

function buildActiveExecutionSummary(
  execution: Execution,
  nowMs: number,
  activityAt: string | undefined,
): string {
  const startedAtMs = new Date(execution.started_at).getTime()
  const heartbeatAtMs = activityAt ? new Date(activityAt).getTime() : startedAtMs
  const parts = [
    execution.claude_num_turns ? `${execution.claude_num_turns} turns` : null,
    formatLiveDuration(Math.max(0, nowMs - startedAtMs)),
    execution.claude_total_cost_usd ? formatCost(execution.claude_total_cost_usd) : null,
    formatHeartbeatAge(Math.max(0, nowMs - heartbeatAtMs)),
  ].filter(Boolean)
  return parts.join(' · ')
}

function mergeActivityTimestamp(
  current: Record<string, string>,
  executionId: string,
  timestamp: string,
): Record<string, string> {
  const existing = current[executionId]
  if (existing && existing >= timestamp) return current
  return { ...current, [executionId]: timestamp }
}

function isViewableAttachment(attachment: MessageAttachment): boolean {
  if (!attachment.path) return false
  if (attachment.content_type === 'application/pdf') return true
  if (attachment.content_type === 'text/html') return true
  if (attachment.content_type.startsWith('image/')) return true
  if (attachment.content_type.startsWith('text/')) return true
  return false
}

function renderAttachmentViewer(attachment: MessageAttachment) {
  if (!attachment.path) return null
  if (attachment.content_type.startsWith('image/')) {
    return (
      <img
        src={attachment.path}
        alt={attachment.filename}
        className="max-w-full max-h-full object-contain rounded"
      />
    )
  }
  return (
    <iframe
      src={attachment.path}
      title={attachment.filename}
      className="w-full h-full rounded bg-white"
      sandbox="allow-same-origin"
    />
  )
}

export default function Conversation() {
  const { sessionId, executionId, proxyExecutionId, conversationExecutionId } = useParams<{
    sessionId: string
    executionId?: string
    proxyExecutionId?: string
    conversationExecutionId?: string
  }>()
  const navigate = useNavigate()

  const [session, setSession] = useState<Session | null>(null)
  const [messages, setMessages] = useState<Message[]>([])
  const [executions, setExecutions] = useState<Execution[]>([])
  const [body, setBody] = useState('')
  const [attachments, setAttachments] = useState<Attachment[]>([])
  const [viewerAttachment, setViewerAttachment] = useState<MessageAttachment | null>(null)
  const [sending, setSending] = useState(false)
  const [stopping, setStopping] = useState(false)
  const [followupHint, setFollowupHint] = useState<FollowupAction | null>(null)
  const [sendError, setSendError] = useState<string | null>(null)
  const [executionActivityAt, setExecutionActivityAt] = useState<Record<string, string>>({})
  const [nowMs, setNowMs] = useState(() => Date.now())
  const bottomRef = useRef<HTMLDivElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const fileRef = useRef<HTMLInputElement>(null)

  const handleFiles = async (files: FileList | null) => {
    if (!files) return
    const result: Attachment[] = []
    for (const f of Array.from(files)) {
      const data = await readAsBase64(f)
      result.push({ filename: f.name, content_type: f.type || 'application/octet-stream', data })
    }
    setAttachments((prev) => [...prev, ...result])
  }

  useEffect(() => {
    if (!sessionId) return
    const loadSession = () => getSession(sessionId).then(setSession).catch(console.error)
    loadSession()
    getMessages(sessionId).then(setMessages).catch(console.error)
    listExecutions(sessionId)
      .then((nextExecutions) => {
        setExecutions(nextExecutions)
        setExecutionActivityAt((prev) => {
          let next = prev
          for (const execution of nextExecutions) {
            const timestamp = execution.halted_at ?? execution.started_at
            next = mergeActivityTimestamp(next, execution.execution_id, timestamp)
          }
          return next
        })
      })
      .catch(console.error)
    const interval = window.setInterval(loadSession, 5000)

    const unsub = subscribeToSession(sessionId, (event: SSEEvent) => {
      if (event.type === 'message') {
        setMessages((prev) =>
          upsertMessage(prev, {
            message_id: event.message_id,
            direction: event.direction,
            body: event.body,
            received_at: event.received_at,
            attachments: event.attachments,
            acknowledged_at: event.acknowledged_at,
            delivery_status: event.delivery_status,
            delivery_error: event.delivery_error,
          }),
        )
      } else if (event.type === 'message_acknowledged') {
        setMessages((prev) => acknowledgeMessage(prev, event.message_id, event.acknowledged_at))
        setFollowupHint((prev) =>
          prev === 'interrupt_after_turn' || prev === 'queued' ? null : prev,
        )
      } else if (event.type === 'followup') {
        setFollowupHint(event.action)
      } else if (event.type === 'status') {
        setSession((prev) =>
          prev ? { ...prev, state: event.state as SessionState } : prev,
        )
      } else if (event.type === 'execution') {
        setFollowupHint(null)
        setExecutionActivityAt((prev) =>
          mergeActivityTimestamp(
            prev,
            event.execution.execution_id,
            event.execution.halted_at ?? new Date().toISOString(),
          ),
        )
        setExecutions((prev) => {
          const idx = prev.findIndex((e) => e.execution_id === event.execution.execution_id)
          if (idx >= 0) {
            const updated = [...prev]
            updated[idx] = event.execution
            return updated
          }
          return [...prev, event.execution]
        })
        getSession(sessionId).then(setSession).catch(console.error)
      } else if (event.type === 'log' || event.type === 'proxy_log' || event.type === 'conversation_event') {
        setExecutionActivityAt((prev) =>
          mergeActivityTimestamp(prev, event.execution_id, event.logged_at),
        )
      }
    })
    return () => {
      window.clearInterval(interval)
      unsub()
    }
  }, [sessionId])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, executions])

  useEffect(() => {
    const hasActiveExecution = executions.some((execution) => execution.halted_at === null)
    if (!hasActiveExecution) return
    const interval = window.setInterval(() => setNowMs(Date.now()), 1000)
    return () => window.clearInterval(interval)
  }, [executions])

  const handleSend = async () => {
    const trimmed = body.trim()
    if (!trimmed || sending || !sessionId) return
    setSending(true)
    setSendError(null)
    try {
      const result = await sendMessage(sessionId, trimmed, attachments)
      setMessages((prev) => upsertMessage(prev, result.message))
      setFollowupHint(result.followup_action)
      setBody('')
      setAttachments([])
      textareaRef.current?.focus()
    } catch (e) {
      console.error(e)
      if (e instanceof ApiError && e.code === 'session_busy') {
        setSendError('This workflow is not accepting a follow-up right now.')
      } else if (e instanceof Error) {
        setSendError(e.message)
      } else {
        setSendError('Failed to send message.')
      }
    } finally {
      setSending(false)
    }
  }

  const handleStop = async () => {
    if (stopping || !sessionId) return
    setStopping(true)
    try {
      await stopExecution(sessionId)
    } catch (e) {
      console.error(e)
    } finally {
      setStopping(false)
    }
  }

  const handleInterruptImmediately = async () => {
    if (stopping || !sessionId) return
    setFollowupHint('interrupting')
    setStopping(true)
    try {
      await stopExecution(sessionId)
    } catch (e) {
      console.error(e)
      setFollowupHint('interrupt_after_turn')
    } finally {
      setStopping(false)
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  const timeline = buildTimeline(messages, executions, executionActivityAt)
  const hasActiveExecution = executions.some((execution) => execution.halted_at === null)

  // Find the execution for the log panel based on the URL param
  const logExecution = executionId
    ? executions.find((e) => e.execution_id === executionId) ?? null
    : null
  const proxyExecution = proxyExecutionId
    ? executions.find((e) => e.execution_id === proxyExecutionId) ?? null
    : null
  const conversationExecution = conversationExecutionId
    ? executions.find((e) => e.execution_id === conversationExecutionId) ?? null
    : null

  const openLogs = (e: Execution) =>
    navigate(`/sessions/${sessionId}/logs/${e.execution_id}`)
  const openProxy = (e: Execution) =>
    navigate(`/sessions/${sessionId}/proxy/${e.execution_id}`)
  const openConversation = (e: Execution) =>
    navigate(`/sessions/${sessionId}/conversation/${e.execution_id}`)
  const closeLogs = () => navigate(`/sessions/${sessionId}`)

  return (
    <div className="h-screen bg-gray-950 text-gray-300 flex flex-col">
      {/* Header */}
      <div className="flex items-center gap-3 px-6 py-3 border-b border-gray-800 bg-gray-900">
        <button
          onClick={() => navigate('/sessions')}
          className="text-blue-400 hover:text-blue-300 text-sm"
        >
          ← Sessions
        </button>
        <span className="text-gray-600">·</span>
        <span className="font-mono text-xs text-gray-400">{sessionId?.slice(0, 8)}</span>
        {session && (
          <>
            <span className="text-gray-600">·</span>
            <span className="text-gray-400 text-sm">{session.workflow_name}</span>
            {[
              session.claude_summary.num_turns ? `${session.claude_summary.num_turns} turns` : null,
              formatDuration(session.claude_summary.duration_ms),
              session.claude_summary.total_tokens
                ? `${session.claude_summary.total_tokens.toLocaleString()} tokens`
                : null,
              formatCost(session.claude_summary.total_cost_usd),
            ].filter(Boolean).length > 0 && (
              <>
                <span className="text-gray-600">·</span>
                <span className="text-xs text-gray-500">
                  {[
                    session.claude_summary.num_turns ? `${session.claude_summary.num_turns} turns` : null,
                    formatDuration(session.claude_summary.duration_ms),
                    session.claude_summary.total_tokens
                      ? `${session.claude_summary.total_tokens.toLocaleString()} tokens`
                      : null,
                    formatCost(session.claude_summary.total_cost_usd),
                  ]
                    .filter(Boolean)
                    .join(' · ')}
                </span>
              </>
            )}
            <span className={`text-sm ml-auto ${STATE_COLOR[session.state] ?? 'text-gray-500'}`}>
              ● {session.state}
            </span>
            {(session.state === 'active' || session.state === 'waiting') && (
              <button
                onClick={handleStop}
                disabled={stopping}
                className="text-xs bg-red-900/50 hover:bg-red-800/60 disabled:opacity-50 text-red-300 border border-red-800 px-2 py-1 rounded"
              >
                {stopping ? 'Stopping…' : 'Stop'}
              </button>
            )}
          </>
        )}
      </div>

      {/* Timeline */}
      <div className="flex-1 overflow-y-auto px-6 py-4 flex flex-col gap-3">
        {timeline.map((item, i) => {
          if (item.kind === 'message') {
            const m = item.data
            const isAssistant = m.direction === 'outbound'
            const displayBody = m.direction === 'error' ? USER_SAFE_ERROR_MESSAGE : m.body
            return (
              <div
                key={item.key}
                className={`max-w-[70%] px-4 py-2 rounded-lg text-sm ${
                  m.direction === 'inbound'
                    ? 'bg-gray-800 self-start text-gray-200'
                    : m.direction === 'error'
                      ? 'bg-red-900/40 self-start text-red-300 border border-red-800'
                      : 'bg-green-900/40 self-end text-gray-100'
                }`}
              >
                {m.direction === 'error' && (
                  <span className="text-red-400 font-medium mr-1">Error:</span>
                )}
                {isAssistant ? (
                  <MarkdownMessage body={displayBody} />
                ) : (
                  <span className="whitespace-pre-wrap">{displayBody}</span>
                )}
                {m.direction === 'inbound' && (
                  <div className="mt-2 text-[11px] text-gray-500">
                    {m.delivery_status === 'pending'
                      ? 'Pending delivery to Claude'
                      : m.delivery_status === 'failed'
                        ? (m.delivery_error ?? 'Delivery failed before Claude received this message')
                        : 'Acknowledged'}
                  </div>
                )}
                {m.attachments && m.attachments.length > 0 && (
                  <div className="mt-2 flex flex-wrap gap-1">
                    {m.attachments.map((a, j) => (
                      isViewableAttachment(a) ? (
                        <button
                          key={j}
                          type="button"
                          onClick={() => setViewerAttachment(a)}
                          className="bg-gray-700 hover:bg-gray-600 text-gray-300 text-xs px-2 py-0.5 rounded"
                          title={`View ${a.filename}`}
                        >
                          📎 {a.filename}
                        </button>
                      ) : (
                        <a
                          key={j}
                          href={a.path}
                          target="_blank"
                          rel="noreferrer"
                          className="bg-gray-700 hover:bg-gray-600 text-gray-300 text-xs px-2 py-0.5 rounded"
                          title={a.content_type}
                        >
                          📎 {a.filename}
                        </a>
                      )
                    ))}
                  </div>
                )}
              </div>
            )
          }

          if (item.kind === 'execution-start') {
            const e = item.data
            const isOpen = e.execution_id === executionId
            const isConversationOpen = e.execution_id === conversationExecutionId
            return (
              <div key={item.key} className="flex items-center gap-3 self-stretch my-1">
                <div className="flex-1 h-px bg-gray-800" />
                <div className="flex flex-col items-center min-w-0">
                  <span className="text-xs text-green-500 whitespace-nowrap">
                    ▶ Execution {e.phase} · {new Date(e.started_at).toLocaleTimeString()}
                  </span>
                </div>
                <button
                  onClick={() => (isOpen ? closeLogs() : openLogs(e))}
                  className={`text-xs underline whitespace-nowrap ${
                    isOpen ? 'text-blue-400 hover:text-blue-300' : 'text-gray-500 hover:text-gray-300'
                  }`}
                >
                  {isOpen ? 'Close logs' : 'View logs'}
                </button>
                <button
                  onClick={() => (e.execution_id === proxyExecutionId ? closeLogs() : openProxy(e))}
                  className={`text-xs underline whitespace-nowrap ${
                    e.execution_id === proxyExecutionId
                      ? 'text-blue-400 hover:text-blue-300'
                      : 'text-gray-500 hover:text-gray-300'
                  }`}
                >
                  {e.execution_id === proxyExecutionId ? 'Close proxy' : 'View proxy'}
                </button>
                <button
                  onClick={() => (isConversationOpen ? closeLogs() : openConversation(e))}
                  className={`text-xs underline whitespace-nowrap ${
                    isConversationOpen
                      ? 'text-blue-400 hover:text-blue-300'
                      : 'text-gray-500 hover:text-gray-300'
                  }`}
                >
                  {isConversationOpen ? 'Close conversation' : 'View conversation'}
                </button>
                <div className="flex-1 h-px bg-gray-800" />
              </div>
            )
          }

          if (item.kind === 'execution-end') {
            const e = item.data
            const isActive = e.halted_at === null
            const failed =
              e.halt_reason === 'tail_error' ||
              (e.exit_code !== null && e.exit_code !== 0) ||
              (e.halt_reason !== 'exited' && e.halt_reason !== null)
            const exitLabel =
              e.halt_reason === 'tail_error'
                ? 'log error'
                : e.exit_code !== null
                  ? `exit ${e.exit_code}`
                  : e.halt_reason ?? ''
            const executionSummary = isActive
              ? buildActiveExecutionSummary(e, nowMs, executionActivityAt[e.execution_id])
              : buildExecutionSummary(e)
            return (
              <div key={item.key} className="flex items-center gap-3 self-stretch my-1">
                <div className={`flex-1 h-px ${isActive ? 'bg-blue-800' : 'bg-gray-800'}`} />
                <div className="flex flex-col items-center min-w-0">
                  <span
                    className={`text-xs whitespace-nowrap ${
                      isActive ? 'text-blue-400' : failed ? 'text-red-500' : 'text-gray-500'
                    }`}
                  >
                    {isActive
                      ? `■ Execution active · ${e.phase}`
                      : `■ Execution ended · ${new Date(e.halted_at!).toLocaleTimeString()}${exitLabel ? ` · ${exitLabel}` : ''}`}
                  </span>
                  {executionSummary && (
                    <span
                      className={`text-[11px] whitespace-nowrap ${
                        isActive ? 'text-blue-300' : 'text-gray-500'
                      }`}
                    >
                      {executionSummary}
                    </span>
                  )}
                </div>
                <div className={`flex-1 h-px ${isActive ? 'bg-blue-800' : 'bg-gray-800'}`} />
              </div>
            )
          }

          return null
        })}
        {timeline.length === 0 && (
          <p className="text-gray-600 text-sm self-center mt-8">No messages yet.</p>
        )}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="px-6 py-4 border-t border-gray-800 flex gap-3 items-end">
        <div className="flex-1 flex flex-col gap-2">
          {followupHint === 'interrupt_after_turn' && (
            <div className="flex items-center gap-3 text-xs">
              <span className="text-amber-400">Will interrupt after the current turn completes.</span>
              {hasActiveExecution && (
                <button
                  type="button"
                  onClick={handleInterruptImmediately}
                  disabled={stopping}
                  className="text-amber-300 hover:text-amber-200 disabled:opacity-50 underline underline-offset-2"
                >
                  {stopping ? 'Interrupting…' : 'Interrupt immediately'}
                </button>
              )}
            </div>
          )}
          {followupHint === 'queued' && (
            <div className="text-xs text-gray-500">Follow-up queued for the next execution.</div>
          )}
          {followupHint === 'interrupting' && (
            <div className="text-xs text-amber-400">Interrupting current execution now.</div>
          )}
          {sendError && (
            <div className="text-xs text-red-400">{sendError}</div>
          )}
          <div className="flex gap-3 items-end">
            <button
              onClick={() => fileRef.current?.click()}
              className="bg-gray-800 border border-gray-700 text-gray-400 px-3 py-2 rounded text-sm self-end"
              title="Attach files"
            >
              📎{attachments.length > 0 ? ` ${attachments.length}` : ''}
            </button>
            <input
              ref={fileRef}
              type="file"
              multiple
              className="hidden"
              onChange={(e) => handleFiles(e.target.files)}
            />
            <textarea
              ref={textareaRef}
              value={body}
              onChange={(e) => setBody(e.target.value)}
              onKeyDown={handleKeyDown}
              rows={2}
              placeholder="Type a message… (Enter to send, Shift+Enter for newline)"
              className="flex-1 bg-gray-800 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 resize-none placeholder:text-gray-600"
            />
            <button
              onClick={handleSend}
              disabled={sending || !body.trim()}
              className="bg-green-700 hover:bg-green-600 disabled:opacity-50 text-white px-4 py-2 rounded text-sm self-end"
            >
              {sending ? '…' : 'Send'}
            </button>
          </div>
        </div>
      </div>

      {/* Log panel — only rendered when executionId is in the URL */}
      {logExecution && (
        <LogPanel
          execution={logExecution}
          onClose={closeLogs}
        />
      )}
      {proxyExecution && (
        <ProxyLogPanel
          execution={proxyExecution}
          onClose={closeLogs}
        />
      )}
      {conversationExecution && (
        <ConversationEventPanel
          execution={conversationExecution}
          onClose={closeLogs}
        />
      )}
      {viewerAttachment && (
        <div
          className="fixed inset-0 bg-black/80 z-50 flex items-center justify-center p-6"
          onClick={() => setViewerAttachment(null)}
        >
          <div
            className="w-full h-full max-w-6xl max-h-[90vh] bg-gray-900 border border-gray-700 rounded-lg shadow-2xl flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center gap-3 px-4 py-3 border-b border-gray-800">
              <div className="min-w-0 flex-1">
                <p className="text-sm text-gray-100 truncate">{viewerAttachment.filename}</p>
                <p className="text-xs text-gray-500">{viewerAttachment.content_type}</p>
              </div>
              <a
                href={viewerAttachment.path}
                target="_blank"
                rel="noreferrer"
                className="text-xs text-blue-400 hover:text-blue-300"
              >
                Open in new tab
              </a>
              <button
                type="button"
                onClick={() => setViewerAttachment(null)}
                className="text-sm text-gray-400 hover:text-gray-200"
              >
                Close
              </button>
            </div>
            <div className="flex-1 p-4 overflow-hidden">
              {renderAttachmentViewer(viewerAttachment)}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
