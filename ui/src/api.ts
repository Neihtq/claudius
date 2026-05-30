import type {
  Attachment,
  ConversationEvent,
  Execution,
  FollowupAction,
  LogLine,
  Message,
  ProxyLog,
  Session,
  SSEEvent,
} from './types'

export class ApiError extends Error {
  status: number
  code?: string

  constructor(message: string, status: number, code?: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
  }
}

async function readApiError(r: Response): Promise<ApiError> {
  let message = `Request failed with status ${r.status}`
  let code: string | undefined
  try {
    const detail = await r.json()
    if (typeof detail?.detail === 'string') {
      message = detail.detail
    } else if (detail?.detail && typeof detail.detail === 'object') {
      message = detail.detail.message ?? message
      code = detail.detail.code
    }
  } catch {
    try {
      const text = await r.text()
      if (text.trim()) message = text
    } catch {
      // ignore secondary parse failure
    }
  }
  return new ApiError(message, r.status, code)
}

export async function listSessions(): Promise<Session[]> {
  const r = await fetch('/sessions')
  if (!r.ok) throw new Error(await r.text())
  return r.json()
}

export async function listWorkflows(): Promise<string[]> {
  const r = await fetch('/workflows')
  if (!r.ok) throw new Error(await r.text())
  return r.json()
}

export async function getSession(id: string): Promise<Session> {
  const r = await fetch(`/sessions/${id}`)
  if (!r.ok) throw new Error(await r.text())
  return r.json()
}

export async function getMessages(id: string): Promise<Message[]> {
  const r = await fetch(`/sessions/${id}/messages`)
  if (!r.ok) throw new Error(await r.text())
  return r.json()
}

export interface InjectPayload {
  channel: string
  sender: string
  recipients?: string[]
  subject?: string
  body: string
  attachments: Attachment[]
}

export async function devInject(payload: InjectPayload): Promise<{ session_id: string }> {
  const r = await fetch('/dev/inject', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (!r.ok) throw new Error(await r.text())
  return r.json()
}

export async function sendMessage(
  sessionId: string,
  body: string,
  attachments: Attachment[] = [],
): Promise<{ message: Message; followup_action: FollowupAction }> {
  const r = await fetch(`/sessions/${sessionId}/message`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ body, attachments }),
  })
  if (!r.ok) throw await readApiError(r)
  return r.json()
}

export async function stopExecution(sessionId: string): Promise<void> {
  const r = await fetch(`/sessions/${sessionId}/execution`, { method: 'DELETE' })
  if (!r.ok) throw new Error(await r.text())
}

export async function deletePendingMessage(sessionId: string, messageId: string): Promise<void> {
  const r = await fetch(`/sessions/${sessionId}/messages/${messageId}`, { method: 'DELETE' })
  if (!r.ok) throw await readApiError(r)
}

export async function resendOutboundMessage(sessionId: string, messageId: string): Promise<void> {
  const r = await fetch(`/sessions/${sessionId}/messages/${messageId}/resend`, { method: 'POST' })
  if (!r.ok) throw await readApiError(r)
}

export async function listExecutions(sessionId: string): Promise<Execution[]> {
  const r = await fetch(`/sessions/${sessionId}/executions`)
  if (!r.ok) throw new Error(await r.text())
  return r.json()
}

export async function getExecutionLogs(executionId: string): Promise<LogLine[]> {
  const r = await fetch(`/executions/${executionId}/logs`)
  if (!r.ok) throw new Error(await r.text())
  return r.json()
}

export async function getProxyLogs(executionId: string): Promise<ProxyLog[]> {
  const r = await fetch(`/executions/${executionId}/proxy-logs`)
  if (!r.ok) throw new Error(await r.text())
  return r.json()
}

export async function getConversationEvents(executionId: string): Promise<ConversationEvent[]> {
  const r = await fetch(`/executions/${executionId}/conversation-events`)
  if (!r.ok) throw new Error(await r.text())
  return r.json()
}

export function subscribeToSession(
  sessionId: string,
  onEvent: (event: SSEEvent) => void,
): () => void {
  const es = new EventSource(`/sessions/${sessionId}/events`)
  es.onmessage = (e) => {
    try {
      onEvent(JSON.parse(e.data) as SSEEvent)
    } catch {
      // ignore malformed events
    }
  }
  return () => es.close()
}
