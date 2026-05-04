export type SessionState = 'new' | 'active' | 'waiting' | 'hibernated' | 'closed' | 'error'
export type ExecutionPhase = 'starting' | 'running' | 'finished' | 'failed'
export type ContainerStatus = 'pending' | 'starting' | 'running' | 'exited' | 'failed'

export interface ClaudeSummary {
  executions_count: number
  completed_executions_count: number
  num_turns: number
  duration_ms: number
  total_cost_usd: number
  input_tokens: number
  output_tokens: number
  cache_creation_input_tokens: number
  cache_read_input_tokens: number
  total_tokens: number
}

export interface Session {
  session_id: string
  thread_id: string
  channel: string
  workflow_name: string
  state: SessionState
  created_at: string
  last_message_at: string
  last_execution_result: 'ok' | 'failed' | null
  claude_summary: ClaudeSummary
}

export interface MessageAttachment {
  filename: string
  content_type: string
  path?: string
}

export interface Message {
  message_id: string
  direction: 'inbound' | 'outbound' | 'error'
  body: string
  received_at: string
  attachments?: MessageAttachment[]
  acknowledged_at: string | null
  delivery_status: 'pending' | 'acknowledged' | 'failed'
  delivery_error?: string | null
}

export interface Attachment {
  filename: string
  content_type: string
  data: string
}

export interface Execution {
  execution_id: string
  session_id: string
  worker_address: string | null
  started_at: string
  halted_at: string | null
  halt_reason: string | null
  phase: ExecutionPhase
  runtime_container: ExecutionContainer | null
  worker_container: ExecutionContainer | null
  exit_code: number | null
  claude_session_id: string | null
  claude_num_turns: number | null
  claude_duration_ms: number | null
  claude_total_cost_usd: number | null
  claude_input_tokens: number | null
  claude_output_tokens: number | null
  claude_cache_creation_input_tokens: number | null
  claude_cache_read_input_tokens: number | null
  claude_total_tokens: number | null
}

export interface ExecutionContainer {
  name: string
  status: ContainerStatus
  healthy: boolean
  host: string | null
  port: number | null
  probe_mode: string | null
}

export interface LogLine {
  logged_at: string
  stream: 'stdout' | 'stderr'
  body: string
}

export interface ProxyLog {
  logged_at: string
  stage: 'request_in' | 'request_out' | 'response_in' | 'response_out'
  method: string | null
  path: string | null
  upstream_url: string | null
  status_code: number | null
  content_type: string | null
  body: string
  meta: Record<string, unknown>
  input_tokens: number | null
  output_tokens: number | null
  cache_creation_input_tokens: number | null
  cache_read_input_tokens: number | null
  total_tokens: number | null
  total_cost_usd: number | null
  request_id: string | null
}

export interface ConversationEvent {
  execution_id: string
  logged_at: string
  seq: number
  source: 'local' | 'claude'
  event_type: string
  event_subtype: string | null
  payload: Record<string, unknown>
}

export type FollowupAction = 'queued' | 'interrupting' | 'interrupt_after_turn' | 'resumed'

export type SSEEvent =
  | {
      type: 'message'
      message_id: string
      direction: 'inbound' | 'outbound' | 'error'
      body: string
      received_at: string
      attachments?: MessageAttachment[]
      acknowledged_at: string | null
      delivery_status: 'pending' | 'acknowledged' | 'failed'
      delivery_error?: string | null
    }
  | {
      type: 'message_acknowledged'
      message_id: string
      acknowledged_at: string
      delivery_status: 'acknowledged'
    }
  | { type: 'followup'; action: FollowupAction }
  | { type: 'status'; state: SessionState }
  | { type: 'ping' }
  | { type: 'execution'; execution: Execution }  // execution includes exit_code
  | { type: 'log'; execution_id: string; logged_at: string; stream: 'stdout' | 'stderr'; body: string }
  | ({ type: 'conversation_event' } & ConversationEvent)
  | {
      type: 'proxy_log'
      execution_id: string
      logged_at: string
      stage: 'request_in' | 'request_out' | 'response_in' | 'response_out'
      method: string | null
      path: string | null
      upstream_url: string | null
      status_code: number | null
      content_type: string | null
      body: string
      meta: Record<string, unknown>
      input_tokens: number | null
      output_tokens: number | null
      cache_creation_input_tokens: number | null
      cache_read_input_tokens: number | null
      total_tokens: number | null
      total_cost_usd: number | null
      request_id: string | null
    }
