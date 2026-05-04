import { useEffect, useMemo, useRef, useState } from 'react'
import { getConversationEvents, subscribeToSession } from '../api'
import type { ConversationEvent, Execution, SSEEvent } from '../types'

interface Props {
  execution: Execution
  onClose: () => void
}

type EntryKind =
  | 'input'
  | 'meta'
  | 'status'
  | 'reasoning'
  | 'tool_call'
  | 'assistant_message'
  | 'run_result'

interface NormalizedEntryBase {
  id: string
  kind: EntryKind
  seq: number
  rawPayloads: Record<string, unknown>[]
}

interface InputEntry extends NormalizedEntryBase {
  kind: 'input'
  title: string
  sender?: string
  body: string
  attachments: Array<{ filename: string; content_type: string }>
}

interface MetaEntry extends NormalizedEntryBase {
  kind: 'meta'
  model?: string
  cwd?: string
  claudeSessionId?: string
  claudeCodeVersion?: string
  toolCount?: number
}

interface StatusEntry extends NormalizedEntryBase {
  kind: 'status'
  status: string
}

interface ReasoningEntry extends NormalizedEntryBase {
  kind: 'reasoning'
  title: string
  body?: string
  redacted: boolean
}

interface ToolCallEntry extends NormalizedEntryBase {
  kind: 'tool_call'
  toolUseId?: string
  toolName: string
  requestInput?: Record<string, unknown>
  requestJsonBuffer?: string
  requestPreview: string
  resultPreview?: string
  resultBody?: string
  status: 'assembling' | 'waiting' | 'complete'
  stopReason?: string
}

interface AssistantMessageEntry extends NormalizedEntryBase {
  kind: 'assistant_message'
  body: string
}

interface RunResultEntry extends NormalizedEntryBase {
  kind: 'run_result'
  terminalReason?: string
  durationMs?: number
  numTurns?: number
  totalCostUsd?: number
  stopReason?: string
}

type NormalizedEntry =
  | InputEntry
  | MetaEntry
  | StatusEntry
  | ReasoningEntry
  | ToolCallEntry
  | AssistantMessageEntry
  | RunResultEntry

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === 'object' && value !== null ? (value as Record<string, unknown>) : null
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : []
}

function getString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined
}

function getNumber(value: unknown): number | undefined {
  return typeof value === 'number' ? value : undefined
}

function getAttachments(value: unknown): Array<{ filename: string; content_type: string }> {
  return asArray(value).flatMap((attachment) => {
    const att = asRecord(attachment)
    const filename = getString(att?.filename)
    const contentType = getString(att?.content_type)
    return filename && contentType ? [{ filename, content_type: contentType }] : []
  })
}

function extractTextContent(content: unknown): string {
  return asArray(content)
    .flatMap((block) => {
      const item = asRecord(block)
      return item?.type === 'text' && typeof item.text === 'string' ? [item.text] : []
    })
    .join('')
    .trim()
}

function extractToolResultContent(content: unknown): string {
  if (typeof content === 'string') return content
  if (!Array.isArray(content)) return ''
  return content
    .flatMap((item) => {
      if (typeof item === 'string') return [item]
      const block = asRecord(item)
      if (block?.type === 'text' && typeof block.text === 'string') return [block.text]
      return []
    })
    .join('')
    .trim()
}

function truncate(value: string | undefined, max = 240): string {
  if (!value) return ''
  return value.length > max ? `${value.slice(0, max)}...` : value
}

function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2)
}

function formatToolRequestPreview(toolName: string, input?: Record<string, unknown>, requestJsonBuffer?: string): string {
  if (toolName === 'Bash') {
    const command = getString(input?.command)
    const description = getString(input?.description)
    if (command && description) return `${command}\n\n${description}`
    if (command) return command
  }
  if (toolName === 'Read') {
    const filePath = getString(input?.file_path)
    const offset = getNumber(input?.offset)
    const limit = getNumber(input?.limit)
    if (filePath) {
      const parts = [filePath]
      if (offset !== undefined) parts.push(`offset ${offset}`)
      if (limit !== undefined) parts.push(`limit ${limit}`)
      return parts.join(' · ')
    }
  }
  if (input && Object.keys(input).length > 0) return formatJson(input)
  return requestJsonBuffer ?? ''
}

function formatCost(value?: number): string | undefined {
  return value !== undefined ? `$${value.toFixed(4)}` : undefined
}

function renderToolCallBody(
  entry: ToolCallEntry,
  bodyExpanded: boolean,
  toggleExpand: () => void,
) {
  const input = entry.requestInput

  if (entry.toolName === 'Bash') {
    const command = getString(input?.command)
    const description = getString(input?.description)
    const resultPreviewText = entry.resultPreview ? truncate(entry.resultPreview, 320) : ''
    return (
      <>
        {command ? (
          <div className="mb-3">
            <div className="text-xs text-gray-500 mb-1">Command</div>
            <div className="rounded border border-gray-800 bg-gray-950 px-3 py-2">
              <pre className="m-0 font-mono text-sm text-gray-200 whitespace-pre-wrap break-words">
                <span className="text-gray-600 select-none">$ </span>{command}
              </pre>
            </div>
            {description && <div className="mt-1.5 text-xs text-gray-500">{description}</div>}
          </div>
        ) : entry.requestPreview ? (
          <div className="mb-3">
            <div className="text-xs text-gray-500 mb-1">Command</div>
            <pre className="m-0 font-mono text-sm text-gray-400 whitespace-pre-wrap break-words">
              {truncate(entry.requestPreview, 260)}
            </pre>
          </div>
        ) : null}
        {entry.resultPreview && (
          <div>
            <div className="text-xs text-gray-500 mb-1">Output</div>
            <pre className={`m-0 font-mono text-xs text-gray-300 whitespace-pre-wrap break-words rounded border border-gray-800 bg-gray-950/50 p-3${bodyExpanded ? '' : ' max-h-64 overflow-auto'}`}>
              {bodyExpanded ? entry.resultBody : resultPreviewText}
            </pre>
            {entry.resultBody && entry.resultBody.length > resultPreviewText.length && (
              <button type="button" onClick={toggleExpand} className="mt-2 text-xs text-blue-400/80 hover:text-blue-300">
                {bodyExpanded ? 'Collapse output' : 'Expand output'}
              </button>
            )}
          </div>
        )}
      </>
    )
  }

  if (entry.toolName === 'Read') {
    const filePath = getString(input?.file_path)
    const offset = getNumber(input?.offset)
    const limit = getNumber(input?.limit)
    const resultPreviewText = entry.resultPreview ? truncate(entry.resultPreview, 320) : ''
    return (
      <>
        {filePath ? (
          <div className="mb-3">
            <div className="text-xs text-gray-500 mb-1">File</div>
            <code className="font-mono text-sm text-cyan-300 break-all">{filePath}</code>
            {(offset !== undefined || limit !== undefined) && (
              <div className="mt-1 text-xs text-gray-500">
                {[offset !== undefined && `offset ${offset}`, limit !== undefined && `limit ${limit}`]
                  .filter(Boolean)
                  .join(' · ')}
              </div>
            )}
          </div>
        ) : entry.requestPreview ? (
          <div className="mb-3">
            <div className="text-xs text-gray-500 mb-1">File</div>
            <pre className="m-0 font-mono text-sm text-gray-400 whitespace-pre-wrap break-words">
              {truncate(entry.requestPreview, 260)}
            </pre>
          </div>
        ) : null}
        {entry.resultPreview && (
          <div>
            <div className="text-xs text-gray-500 mb-1">Content</div>
            <pre className={`m-0 font-mono text-xs text-gray-300 whitespace-pre-wrap break-words rounded border border-gray-800 bg-gray-950/50 p-3${bodyExpanded ? '' : ' max-h-64 overflow-auto'}`}>
              {bodyExpanded ? entry.resultBody : resultPreviewText}
            </pre>
            {entry.resultBody && entry.resultBody.length > resultPreviewText.length && (
              <button type="button" onClick={toggleExpand} className="mt-2 text-xs text-blue-400/80 hover:text-blue-300">
                {bodyExpanded ? 'Collapse content' : 'Expand content'}
              </button>
            )}
          </div>
        )}
      </>
    )
  }

  if (entry.toolName === 'Write') {
    const filePath = getString(input?.file_path)
    const content = getString(input?.content)
    const lines = content !== undefined ? content.split('\n') : []
    const previewLines = lines.slice(0, 10).join('\n')
    const hasMore = lines.length > 10
    return (
      <>
        {filePath && (
          <div className="mb-3">
            <div className="text-xs text-gray-500 mb-1">File</div>
            <code className="font-mono text-sm text-cyan-300 break-all">{filePath}</code>
          </div>
        )}
        {content !== undefined && (
          <div>
            <div className="text-xs text-gray-500 mb-1">Content</div>
            <pre className={`m-0 font-mono text-xs text-gray-300 whitespace-pre-wrap break-words rounded border border-gray-800 bg-gray-950/50 p-3${bodyExpanded ? '' : ' max-h-64 overflow-auto'}`}>
              {bodyExpanded ? content : previewLines}
            </pre>
            {hasMore && (
              <button type="button" onClick={toggleExpand} className="mt-2 text-xs text-blue-400/80 hover:text-blue-300">
                {bodyExpanded ? `Collapse (${lines.length} lines)` : `Expand (${lines.length} lines)`}
              </button>
            )}
          </div>
        )}
      </>
    )
  }

  if (entry.toolName === 'Edit') {
    const filePath = getString(input?.file_path)
    const oldString = input?.old_string !== undefined ? getString(input.old_string) ?? '' : undefined
    const newString = input?.new_string !== undefined ? getString(input.new_string) ?? '' : undefined
    return (
      <>
        {filePath && (
          <div className="mb-3">
            <div className="text-xs text-gray-500 mb-1">File</div>
            <code className="font-mono text-sm text-cyan-300 break-all">{filePath}</code>
          </div>
        )}
        {(oldString !== undefined || newString !== undefined) && (
          <div>
            <div className="text-xs text-gray-500 mb-1">Changes</div>
            <div className="rounded border border-gray-800 max-h-64 overflow-y-auto">
              {oldString !== undefined && oldString.split('\n').map((line, i) => (
                <div key={`old-${i}`} className="flex bg-red-950/40 px-3 py-0.5">
                  <span className="font-mono text-xs text-red-500 mr-2 select-none shrink-0">-</span>
                  <span className="font-mono text-xs text-red-300 whitespace-pre-wrap break-words">{line}</span>
                </div>
              ))}
              {newString !== undefined && newString.split('\n').map((line, i) => (
                <div key={`new-${i}`} className="flex bg-green-950/40 px-3 py-0.5">
                  <span className="font-mono text-xs text-green-500 mr-2 select-none shrink-0">+</span>
                  <span className="font-mono text-xs text-green-300 whitespace-pre-wrap break-words">{line}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </>
    )
  }

  if (entry.toolName === 'TodoWrite') {
    type TodoItem = { id?: string; content?: string; status?: string; priority?: string }
    const todos: TodoItem[] = Array.isArray(input?.todos) ? (input.todos as TodoItem[]) : []

    const statusIcon = (status?: string): { icon: string; color: string } => {
      if (status === 'completed') return { icon: '✓', color: 'text-green-400' }
      if (status === 'in_progress') return { icon: '◐', color: 'text-yellow-400' }
      return { icon: '○', color: 'text-gray-500' }
    }

    const priorityClass = (priority?: string): string => {
      if (priority === 'high') return 'bg-red-900/50 text-red-300'
      if (priority === 'medium') return 'bg-yellow-900/50 text-yellow-300'
      return 'bg-gray-800 text-gray-400'
    }

    return (
      <div className="flex flex-col gap-2">
        {todos.length === 0 && <span className="text-sm text-gray-500">No todos</span>}
        {todos.map((todo, i) => {
          const { icon, color } = statusIcon(todo.status)
          return (
            <div key={todo.id ?? i} className="flex items-start gap-2">
              <span className={`${color} text-sm shrink-0 mt-0.5`}>{icon}</span>
              <span className="text-sm text-gray-200 flex-1">{todo.content}</span>
              {todo.priority && (
                <span className={`text-xs px-1.5 py-0.5 rounded shrink-0 ${priorityClass(todo.priority)}`}>
                  {todo.priority}
                </span>
              )}
            </div>
          )
        })}
      </div>
    )
  }

  // Default: generic rendering
  const requestPreview = truncate(entry.requestPreview, 260)
  const resultPreviewText = entry.resultPreview ? truncate(entry.resultPreview, 320) : ''
  return (
    <>
      {requestPreview && (
        <div className="mb-3">
          <div className="text-xs text-gray-500 mb-1">Request</div>
          <pre className="m-0 whitespace-pre-wrap break-words text-sm text-gray-200 font-sans">
            {requestPreview}
          </pre>
        </div>
      )}
      {entry.resultPreview && (
        <div>
          <div className="text-xs text-gray-500 mb-1">Result</div>
          <pre className="m-0 whitespace-pre-wrap break-words text-sm text-gray-300 font-sans">
            {bodyExpanded ? entry.resultBody : resultPreviewText}
          </pre>
          {entry.resultBody && entry.resultBody.length > resultPreviewText.length && (
            <button type="button" onClick={toggleExpand} className="mt-2 text-xs text-blue-400/80 hover:text-blue-300">
              {bodyExpanded ? 'Collapse result' : 'Expand result'}
            </button>
          )}
        </div>
      )}
    </>
  )
}

function appendRawPayload(entry: NormalizedEntry, payload: Record<string, unknown>) {
  entry.rawPayloads.push(payload)
}

function buildTranscript(events: ConversationEvent[]): NormalizedEntry[] {
  const entries: NormalizedEntry[] = []
  const toolEntries = new Map<string, ToolCallEntry>()
  const openToolByBlockIndex = new Map<number, ToolCallEntry>()
  let lastStatus: string | null = null

  const pushEntry = <T extends NormalizedEntry>(entry: T): T => {
    entries.push(entry)
    return entry
  }

  for (const event of events) {
    const payload = event.payload ?? {}
    const message = asRecord(payload.message)
    const streamEvent = asRecord(payload.event)

    if (event.source === 'local' && event.event_type === 'input') {
      pushEntry({
        id: `${event.execution_id}-${event.seq}`,
        kind: 'input',
        seq: event.seq,
        rawPayloads: [payload],
        title: event.event_subtype === 'initial_message' ? 'Initial message' : 'Follow-up message',
        sender: getString(payload.sender),
        body: getString(payload.body) ?? '',
        attachments: getAttachments(payload.attachments),
      })
      continue
    }

    if (event.event_type === 'system') {
      if (event.event_subtype === 'init') {
        pushEntry({
          id: `${event.execution_id}-${event.seq}`,
          kind: 'meta',
          seq: event.seq,
          rawPayloads: [payload],
          model: getString(payload.model),
          cwd: getString(payload.cwd),
          claudeSessionId: getString(payload.session_id),
          claudeCodeVersion: getString(payload.claude_code_version),
          toolCount: asArray(payload.tools).length,
        })
        continue
      }
      if (event.event_subtype === 'status') {
        const status = getString(payload.status)
        if (status && status !== lastStatus) {
          lastStatus = status
          pushEntry({
            id: `${event.execution_id}-${event.seq}`,
            kind: 'status',
            seq: event.seq,
            rawPayloads: [payload],
            status,
          })
        }
        continue
      }
    }

    if (event.event_type === 'stream_event' && streamEvent) {
      const streamType = getString(streamEvent.type)
      const blockIndex = getNumber(streamEvent.index)
      if (streamType === 'content_block_start') {
        const block = asRecord(streamEvent.content_block)
        const blockType = getString(block?.type)
        if (blockType === 'tool_use') {
          const toolUseId = getString(block?.id)
          const toolName = getString(block?.name) ?? 'Tool'
          const input = asRecord(block?.input) ?? undefined
          const entry = pushEntry({
            id: toolUseId ?? `${event.execution_id}-${event.seq}`,
            kind: 'tool_call',
            seq: event.seq,
            rawPayloads: [payload],
            toolUseId,
            toolName,
            requestInput: input,
            requestJsonBuffer: '',
            requestPreview: formatToolRequestPreview(toolName, input),
            status: 'assembling',
          })
          if (toolUseId) toolEntries.set(toolUseId, entry)
          if (blockIndex !== undefined) openToolByBlockIndex.set(blockIndex, entry)
          continue
        }
      }

      if (streamType === 'content_block_delta') {
        const delta = asRecord(streamEvent.delta)
        const deltaType = getString(delta?.type)
        if (deltaType === 'input_json_delta' && blockIndex !== undefined) {
          const entry = openToolByBlockIndex.get(blockIndex)
          const chunk = getString(delta?.partial_json) ?? ''
          if (entry && chunk) {
            entry.requestJsonBuffer = `${entry.requestJsonBuffer ?? ''}${chunk}`
            entry.requestPreview = formatToolRequestPreview(
              entry.toolName,
              entry.requestInput,
              entry.requestJsonBuffer,
            )
            appendRawPayload(entry, payload)
          }
          continue
        }
      }

      if (streamType === 'content_block_stop' && blockIndex !== undefined) {
        openToolByBlockIndex.delete(blockIndex)
        continue
      }

      if (streamType === 'message_delta') {
        const delta = asRecord(streamEvent.delta)
        const stopReason = getString(delta?.stop_reason)
        if (stopReason === 'tool_use') {
          const lastEntry = entries[entries.length - 1]
          if (lastEntry?.kind === 'tool_call') {
            lastEntry.stopReason = stopReason
            appendRawPayload(lastEntry, payload)
          }
        }
        continue
      }

      continue
    }

    if (event.event_type === 'assistant' && message) {
      for (const contentBlock of asArray(message.content)) {
        const block = asRecord(contentBlock)
        const blockType = getString(block?.type)
        if (blockType === 'thinking') {
          pushEntry({
            id: `${event.execution_id}-${event.seq}-thinking`,
            kind: 'reasoning',
            seq: event.seq,
            rawPayloads: [payload],
            title: 'Reasoning',
            body: getString(block?.thinking),
            redacted: false,
          })
          continue
        }
        if (blockType === 'redacted_thinking') {
          pushEntry({
            id: `${event.execution_id}-${event.seq}-redacted`,
            kind: 'reasoning',
            seq: event.seq,
            rawPayloads: [payload],
            title: 'Redacted reasoning',
            body: undefined,
            redacted: true,
          })
          continue
        }
        if (blockType === 'tool_use') {
          const toolUseId = getString(block?.id)
          const toolName = getString(block?.name) ?? 'Tool'
          const input = asRecord(block?.input) ?? undefined
          const existing = toolUseId ? toolEntries.get(toolUseId) : undefined
          if (existing) {
            existing.requestInput = input
            existing.requestPreview = formatToolRequestPreview(toolName, input, existing.requestJsonBuffer)
            existing.status = 'waiting'
            appendRawPayload(existing, payload)
          } else {
            const entry = pushEntry({
              id: toolUseId ?? `${event.execution_id}-${event.seq}-tool`,
              kind: 'tool_call',
              seq: event.seq,
              rawPayloads: [payload],
              toolUseId,
              toolName,
              requestInput: input,
              requestPreview: formatToolRequestPreview(toolName, input),
              status: 'waiting',
            })
            if (toolUseId) toolEntries.set(toolUseId, entry)
          }
          continue
        }
        if (blockType === 'text') {
          const text = getString(block?.text)?.trim()
          if (text) {
            pushEntry({
              id: `${event.execution_id}-${event.seq}-assistant`,
              kind: 'assistant_message',
              seq: event.seq,
              rawPayloads: [payload],
              body: text,
            })
          }
        }
      }
      continue
    }

    if (event.event_type === 'user' && message) {
      for (const contentBlock of asArray(message.content)) {
        const block = asRecord(contentBlock)
        if (getString(block?.type) !== 'tool_result') continue
        const toolUseId = getString(block?.tool_use_id)
        const resultBody = extractToolResultContent(block?.content)
        const resultPreview = truncate(resultBody, 320)
        const existing = toolUseId ? toolEntries.get(toolUseId) : undefined
        if (existing) {
          existing.resultBody = resultBody
          existing.resultPreview = resultPreview
          existing.status = 'complete'
          appendRawPayload(existing, payload)
        } else {
          const entry = pushEntry({
            id: toolUseId ?? `${event.execution_id}-${event.seq}-tool-result`,
            kind: 'tool_call',
            seq: event.seq,
            rawPayloads: [payload],
            toolUseId,
            toolName: 'Tool',
            requestPreview: '',
            resultBody,
            resultPreview,
            status: 'complete',
          })
          if (toolUseId) toolEntries.set(toolUseId, entry)
        }
      }
      continue
    }

    if (event.event_type === 'result') {
      pushEntry({
        id: `${event.execution_id}-${event.seq}`,
        kind: 'run_result',
        seq: event.seq,
        rawPayloads: [payload],
        terminalReason: getString(payload.terminal_reason),
        durationMs: getNumber(payload.duration_ms),
        numTurns: getNumber(payload.num_turns),
        totalCostUsd: getNumber(payload.total_cost_usd),
        stopReason: getString(payload.stop_reason),
      })
    }
  }

  return entries
}

function formatRawPayloads(payloads: Record<string, unknown>[]): string {
  if (payloads.length === 1) return formatJson(payloads[0])
  return payloads
    .map((payload, index) => `Event ${index + 1}\n${formatJson(payload)}`)
    .join('\n\n')
}

function MetaRow({ label, value }: { label: string; value?: string | number }) {
  if (value === undefined || value === '') return null
  return (
    <div className="flex items-center gap-2">
      <span className="text-gray-500">{label}</span>
      <span className="text-gray-300 break-all">{value}</span>
    </div>
  )
}

function getCardClass(kind: Exclude<EntryKind, 'status'>, highlighted: boolean): string {
  const base = 'rounded-lg border px-4 py-3 transition-colors duration-700 ease-out'
  if (kind === 'meta') {
    return `${base} ${highlighted ? 'border-sky-700 bg-sky-950/30' : 'border-gray-800 bg-gray-950/80'}`
  }
  if (kind === 'input') {
    return `${base} ${highlighted ? 'border-sky-700 bg-sky-950/25' : 'border-gray-800 bg-gray-950'}`
  }
  if (kind === 'reasoning') {
    return `${base} ${highlighted ? 'border-amber-700 bg-amber-900/30' : 'border-amber-900/50 bg-amber-950/15'}`
  }
  if (kind === 'tool_call') {
    return `${base} ${highlighted ? 'border-cyan-600 bg-cyan-950/35' : 'border-blue-900/50 bg-blue-950/15'}`
  }
  if (kind === 'assistant_message') {
    return `${base} ${highlighted ? 'border-emerald-700 bg-emerald-950/35' : 'border-green-900/60 bg-green-950/25'}`
  }
  if (kind === 'run_result') {
    return `${base} ${highlighted ? 'border-slate-600 bg-slate-900/35' : 'border-gray-800 bg-gray-950/80'}`
  }
  return base
}

export default function ConversationEventPanel({ execution, onClose }: Props) {
  const [events, setEvents] = useState<ConversationEvent[]>([])
  const [expandedBodies, setExpandedBodies] = useState<string[]>([])
  const [expandedRaw, setExpandedRaw] = useState<string[]>([])
  const [highlightedEntryId, setHighlightedEntryId] = useState<string | null>(null)
  const scrollRef = useRef<HTMLDivElement>(null)
  const entryRefs = useRef<Record<string, HTMLDivElement | null>>({})
  const atBottomRef = useRef(true)
  const didInitialScroll = useRef(false)
  const highlightTimeoutRef = useRef<number | null>(null)

  useEffect(() => {
    const buffered: ConversationEvent[] = []
    let fetched = false

    const unsub = subscribeToSession(execution.session_id, (event: SSEEvent) => {
      if (event.type !== 'conversation_event' || event.execution_id !== execution.execution_id) return
      if (fetched) {
        setEvents((prev) => [...prev, event])
      } else {
        buffered.push(event)
      }
    })

    getConversationEvents(execution.execution_id)
      .then((historical) => {
        fetched = true
        const all = [...historical, ...buffered]
        const seen = new Set<string>()
        const deduped = all.filter((event) => {
          const key = `${event.execution_id}:${event.seq}:${event.event_type}:${event.event_subtype ?? ''}`
          if (seen.has(key)) return false
          seen.add(key)
          return true
        })
        deduped.sort((a, b) => a.seq - b.seq)
        setEvents(deduped)
      })
      .catch(console.error)

    return unsub
  }, [execution.execution_id, execution.session_id])

  const transcript = useMemo(() => buildTranscript(events), [events])

  useEffect(() => {
    return () => {
      if (highlightTimeoutRef.current !== null) {
        window.clearTimeout(highlightTimeoutRef.current)
      }
    }
  }, [])

  const highlightEntry = (id: string) => {
    setHighlightedEntryId(id)
    if (highlightTimeoutRef.current !== null) {
      window.clearTimeout(highlightTimeoutRef.current)
    }
    highlightTimeoutRef.current = window.setTimeout(() => {
      setHighlightedEntryId((current) => (current === id ? null : current))
      highlightTimeoutRef.current = null
    }, 1800)
  }

  const scrollEntryIntoViewIfNeeded = (id: string) => {
    requestAnimationFrame(() => {
      const container = scrollRef.current
      const entry = entryRefs.current[id]
      if (!container || !entry) return
      const containerRect = container.getBoundingClientRect()
      const entryRect = entry.getBoundingClientRect()
      if (entryRect.top < containerRect.top || entryRect.bottom > containerRect.bottom) {
        entry.scrollIntoView({ block: 'start', behavior: 'smooth' })
        atBottomRef.current = false
      }
    })
  }

  const toggleExpandedBody = (id: string) => {
    setExpandedBodies((prev) => {
      const isExpanded = prev.includes(id)
      if (isExpanded) {
        highlightEntry(id)
        scrollEntryIntoViewIfNeeded(id)
        return prev.filter((value) => value !== id)
      }
      highlightEntry(id)
      return [...prev, id]
    })
  }

  const toggleExpandedRaw = (id: string) => {
    setExpandedRaw((prev) => (prev.includes(id) ? prev.filter((value) => value !== id) : [...prev, id]))
  }

  const handleScroll = () => {
    const el = scrollRef.current
    if (!el) return
    atBottomRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 50
  }

  const scrollToBottom = () => {
    const el = scrollRef.current
    if (!el) return
    el.scrollTop = el.scrollHeight
  }

  useEffect(() => {
    if (transcript.length === 0) return
    if (!didInitialScroll.current) {
      didInitialScroll.current = true
      scrollToBottom()
      return
    }
    if (!execution.halted_at && atBottomRef.current) {
      scrollToBottom()
    }
  }, [transcript, execution.halted_at])

  return (
    <div className="fixed inset-0 z-50 flex justify-end" onClick={(e) => e.target === e.currentTarget && onClose()}>
      <div className="w-[620px] max-w-full h-full bg-gray-900 border-l border-gray-700 flex flex-col shadow-2xl">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700 shrink-0">
          <div className="flex items-center gap-3">
            <span className="text-xs text-gray-400 font-mono">
              convo {execution.execution_id.slice(0, 8)}
            </span>
            {execution.halted_at ? (
              <span className="text-xs text-gray-500">
                ■ ended {new Date(execution.halted_at).toLocaleTimeString()}
              </span>
            ) : (
              <span className="text-xs text-green-500">● live</span>
            )}
          </div>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-300 text-lg leading-none px-1">
            ✕
          </button>
        </div>

        <div ref={scrollRef} onScroll={handleScroll} className="flex-1 overflow-y-auto p-4 flex flex-col gap-3">
          {transcript.map((entry) => {
            const bodyExpanded = expandedBodies.includes(entry.id)
            const rawExpanded = expandedRaw.includes(entry.id)
            const highlighted = bodyExpanded || highlightedEntryId === entry.id

            if (entry.kind === 'status') {
              return (
                <div key={entry.id} className="self-center text-xs text-gray-500 uppercase tracking-wide">
                  {entry.status}
                </div>
              )
            }

            if (entry.kind === 'meta') {
              return (
                <div
                  key={entry.id}
                  ref={(el) => { entryRefs.current[entry.id] = el }}
                  className={getCardClass(entry.kind, highlighted)}
                >
                  <div className="text-xs text-gray-400 mb-2">Run context</div>
                  <div className="flex flex-col gap-1 text-xs">
                    <MetaRow label="model" value={entry.model} />
                    <MetaRow label="cwd" value={entry.cwd} />
                    <MetaRow label="session" value={entry.claudeSessionId} />
                    <MetaRow label="claude-code" value={entry.claudeCodeVersion} />
                    <MetaRow label="tools" value={entry.toolCount} />
                  </div>
                  <div className="mt-3">
                    <button
                      type="button"
                      onClick={() => toggleExpandedRaw(entry.id)}
                      className="text-xs text-gray-500 hover:text-gray-300"
                    >
                      {rawExpanded ? 'Hide raw event' : 'Show raw event'}
                    </button>
                    {rawExpanded && (
                      <pre className="mt-2 m-0 max-h-80 overflow-auto rounded border border-gray-800 bg-gray-950 p-3 text-xs whitespace-pre-wrap break-words text-gray-300">
                        {formatRawPayloads(entry.rawPayloads)}
                      </pre>
                    )}
                  </div>
                </div>
              )
            }

            if (entry.kind === 'input') {
              return (
                <div
                  key={entry.id}
                  ref={(el) => { entryRefs.current[entry.id] = el }}
                  className={`max-w-[85%] text-sm ${getCardClass(entry.kind, highlighted)}`}
                >
                  <div className="flex items-center gap-3 text-xs mb-2">
                    <span className="text-gray-300">{entry.title}</span>
                    {entry.sender && <span className="text-gray-500">{entry.sender}</span>}
                  </div>
                  <pre className="m-0 whitespace-pre-wrap break-words text-sm text-gray-200 font-sans">
                    {entry.body}
                  </pre>
                  {entry.attachments.length > 0 && (
                    <div className="mt-2 flex flex-wrap gap-1">
                      {entry.attachments.map((attachment) => (
                        <span
                          key={`${entry.id}-${attachment.filename}`}
                          className="bg-gray-800 text-gray-400 text-xs px-2 py-0.5 rounded"
                        >
                          {attachment.filename}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              )
            }

            if (entry.kind === 'reasoning') {
              const preview = entry.redacted
                ? 'Claude produced a redacted reasoning block.'
                : truncate(entry.body, 220)
              return (
                <div
                  key={entry.id}
                  ref={(el) => { entryRefs.current[entry.id] = el }}
                  className={getCardClass(entry.kind, highlighted)}
                >
                  <div className="flex items-center justify-between gap-3 text-xs">
                    <span className="text-amber-200">{entry.title}</span>
                    <button
                      type="button"
                      onClick={() => toggleExpandedBody(entry.id)}
                      className="text-amber-400/80 hover:text-amber-300"
                    >
                      {bodyExpanded ? 'Hide' : 'Show'}
                    </button>
                  </div>
                  {!bodyExpanded && preview && (
                    <p className="mt-2 text-sm text-amber-100/80">{preview}</p>
                  )}
                  {bodyExpanded && (
                    <div className="mt-2">
                      {entry.body ? (
                        <pre className="m-0 whitespace-pre-wrap break-words text-sm text-amber-50 font-sans">
                          {entry.body}
                        </pre>
                      ) : (
                        <p className="m-0 text-sm text-amber-100/80">Reasoning content is not available in readable form.</p>
                      )}
                    </div>
                  )}
                  <div className="mt-3">
                    <button
                      type="button"
                      onClick={() => toggleExpandedRaw(entry.id)}
                      className="text-xs text-gray-500 hover:text-gray-300"
                    >
                      {rawExpanded ? 'Hide raw event' : 'Show raw event'}
                    </button>
                    {rawExpanded && (
                      <pre className="mt-2 m-0 max-h-80 overflow-auto rounded border border-gray-800 bg-gray-950 p-3 text-xs whitespace-pre-wrap break-words text-gray-300">
                        {formatRawPayloads(entry.rawPayloads)}
                      </pre>
                    )}
                  </div>
                </div>
              )
            }

            if (entry.kind === 'tool_call') {
              return (
                <div
                  key={entry.id}
                  ref={(el) => { entryRefs.current[entry.id] = el }}
                  className={getCardClass(entry.kind, highlighted)}
                >
                  <div className="flex items-center gap-3 text-xs mb-3">
                    <span className="text-blue-200">{entry.toolName}</span>
                    <span className="text-blue-400/80">{entry.status}</span>
                    {entry.stopReason && <span className="text-gray-500">{entry.stopReason}</span>}
                  </div>
                  {renderToolCallBody(entry, bodyExpanded, () => toggleExpandedBody(entry.id))}
                  <div className="mt-3">
                    <button
                      type="button"
                      onClick={() => toggleExpandedRaw(entry.id)}
                      className="text-xs text-gray-500 hover:text-gray-300"
                    >
                      {rawExpanded ? 'Hide raw events' : 'Show raw events'}
                    </button>
                    {rawExpanded && (
                      <pre className="mt-2 m-0 max-h-80 overflow-auto rounded border border-gray-800 bg-gray-950 p-3 text-xs whitespace-pre-wrap break-words text-gray-300">
                        {formatRawPayloads(entry.rawPayloads)}
                      </pre>
                    )}
                  </div>
                </div>
              )
            }

            if (entry.kind === 'assistant_message') {
              return (
                <div
                  key={entry.id}
                  ref={(el) => { entryRefs.current[entry.id] = el }}
                  className={`self-end max-w-[85%] ${getCardClass(entry.kind, highlighted)}`}
                >
                  <div className="text-xs text-green-300 mb-2">Claude</div>
                  <pre className="m-0 whitespace-pre-wrap break-words text-sm text-gray-100 font-sans">
                    {entry.body}
                  </pre>
                  <div className="mt-3">
                    <button
                      type="button"
                      onClick={() => toggleExpandedRaw(entry.id)}
                      className="text-xs text-gray-500 hover:text-gray-300"
                    >
                      {rawExpanded ? 'Hide raw event' : 'Show raw event'}
                    </button>
                    {rawExpanded && (
                      <pre className="mt-2 m-0 max-h-80 overflow-auto rounded border border-gray-800 bg-gray-950 p-3 text-xs whitespace-pre-wrap break-words text-gray-300">
                        {formatRawPayloads(entry.rawPayloads)}
                      </pre>
                    )}
                  </div>
                </div>
              )
            }

            if (entry.kind === 'run_result') {
              return (
                <div
                  key={entry.id}
                  ref={(el) => { entryRefs.current[entry.id] = el }}
                  className={`text-xs ${getCardClass(entry.kind, highlighted)}`}
                >
                  <div className="text-gray-300 mb-2">Run complete</div>
                  <div className="flex flex-wrap gap-x-4 gap-y-1 text-gray-400">
                    <span>terminal: {entry.terminalReason ?? 'unknown'}</span>
                    {entry.stopReason && <span>stop: {entry.stopReason}</span>}
                    {entry.numTurns !== undefined && <span>turns: {entry.numTurns}</span>}
                    {entry.durationMs !== undefined && <span>duration: {(entry.durationMs / 1000).toFixed(1)}s</span>}
                    {formatCost(entry.totalCostUsd) && <span>cost: {formatCost(entry.totalCostUsd)}</span>}
                  </div>
                  <div className="mt-3">
                    <button
                      type="button"
                      onClick={() => toggleExpandedRaw(entry.id)}
                      className="text-xs text-gray-500 hover:text-gray-300"
                    >
                      {rawExpanded ? 'Hide raw event' : 'Show raw event'}
                    </button>
                    {rawExpanded && (
                      <pre className="mt-2 m-0 max-h-80 overflow-auto rounded border border-gray-800 bg-gray-950 p-3 text-xs whitespace-pre-wrap break-words text-gray-300">
                        {formatRawPayloads(entry.rawPayloads)}
                      </pre>
                    )}
                  </div>
                </div>
              )
            }

            return null
          })}
          {transcript.length === 0 && <p className="text-gray-600 mt-2">No conversation events yet.</p>}
        </div>
      </div>
    </div>
  )
}
