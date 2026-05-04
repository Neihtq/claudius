import { useEffect, useRef, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { getExecutionLogs, subscribeToSession } from '../api'
import type { Execution, LogLine, SSEEvent } from '../types'

interface Props {
  execution: Execution
  onClose: () => void
}

/** Parse #L5 or #L5-L10 from the URL hash. Returns [start, end] (1-based, inclusive). */
function parseLineHash(hash: string): [number, number] | null {
  const m = hash.match(/^#L(\d+)(?:-L(\d+))?$/)
  if (!m) return null
  const start = parseInt(m[1], 10)
  const end = m[2] ? parseInt(m[2], 10) : start
  return [Math.min(start, end), Math.max(start, end)]
}

export default function LogPanel({ execution, onClose }: Props) {
  const navigate = useNavigate()
  const { hash } = useLocation()
  const [lines, setLines] = useState<LogLine[]>([])
  const scrollRef = useRef<HTMLDivElement>(null)
  const lineRefs = useRef<(HTMLDivElement | null)[]>([])
  const atBottomRef = useRef(true)
  const didInitialScroll = useRef(false)

  const selectedRange = parseLineHash(hash)

  const handleScroll = () => {
    const el = scrollRef.current
    if (!el) return
    atBottomRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 50
  }

  const scrollToBottom = () => {
    const el = scrollRef.current
    if (el) el.scrollTop = el.scrollHeight
  }

  useEffect(() => {
    const buffered: LogLine[] = []
    let fetched = false

    const unsub = subscribeToSession(execution.session_id, (event: SSEEvent) => {
      if (event.type !== 'log' || event.execution_id !== execution.execution_id) return
      const line: LogLine = { logged_at: event.logged_at, stream: event.stream, body: event.body }
      if (fetched) {
        setLines((prev) => [...prev, line])
      } else {
        buffered.push(line)
      }
    })

    getExecutionLogs(execution.execution_id)
      .then((historical) => {
        fetched = true
        const all = [...historical, ...buffered]
        const seen = new Set<string>()
        const deduped = all.filter((l) => {
          const key = `${l.logged_at}|${l.stream}|${l.body}`
          if (seen.has(key)) return false
          seen.add(key)
          return true
        })
        deduped.sort((a, b) => a.logged_at.localeCompare(b.logged_at))
        setLines(deduped)
      })
      .catch(console.error)

    return unsub
  }, [execution.execution_id, execution.session_id])

  // Initial jump to bottom once lines are populated; then follow if live and at bottom
  useEffect(() => {
    if (lines.length === 0) return
    if (!didInitialScroll.current) {
      didInitialScroll.current = true
      scrollToBottom()
      return
    }
    if (!execution.halted_at && atBottomRef.current) {
      scrollToBottom()
    }
  }, [lines, execution.halted_at])

  // Scroll to selected line when hash changes
  useEffect(() => {
    if (selectedRange) {
      lineRefs.current[selectedRange[0] - 1]?.scrollIntoView({ behavior: 'smooth', block: 'center' })
    }
  }, [selectedRange])

  const handleLineClick = (lineNum: number) => {
    const [s, e] = selectedRange ?? [0, 0]
    if (selectedRange && lineNum >= s && lineNum <= e) {
      // Clicking inside the selection clears it
      navigate({ hash: '' }, { replace: true })
    } else {
      navigate({ hash: `#L${lineNum}` }, { replace: true })
    }
  }

  const handleBackdropClick = (e: React.MouseEvent) => {
    if (e.target === e.currentTarget) onClose()
  }

  const isSelected = (lineNum: number) =>
    selectedRange !== null && lineNum >= selectedRange[0] && lineNum <= selectedRange[1]

  const runtimeLabel = execution.runtime_container
    ? `runtime ${execution.runtime_container.status}${execution.runtime_container.healthy ? ' healthy' : ''}`
    : 'runtime unavailable'
  const workerLabel = execution.worker_container
    ? `worker ${execution.worker_container.status}${execution.worker_container.healthy ? ' healthy' : ''}`
    : 'worker unavailable'
  const runtimeTarget =
    execution.runtime_container?.host && execution.runtime_container?.port
      ? `${execution.runtime_container.host}:${execution.runtime_container.port}`
      : null

  return (
    <div
      className="fixed inset-0 z-50 flex justify-end"
      onClick={handleBackdropClick}
    >
      <div className="w-[480px] max-w-full h-full bg-gray-900 border-l border-gray-700 flex flex-col shadow-2xl">
        {/* Panel header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700 shrink-0">
          <div className="flex items-center gap-3">
            <span className="text-xs text-gray-400 font-mono">
              exec {execution.execution_id.slice(0, 8)}
            </span>
            <span className="text-xs text-blue-400">phase {execution.phase}</span>
            {execution.halted_at ? (
              <span className="text-xs text-gray-500">
                ■ ended {new Date(execution.halted_at).toLocaleTimeString()}
                {execution.halt_reason ? ` · ${execution.halt_reason}` : ''}
              </span>
            ) : (
              <span className="text-xs text-green-500">● live</span>
            )}
          </div>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-gray-300 text-lg leading-none px-1"
          >
            ✕
          </button>
        </div>
        <div className="px-4 py-2 border-b border-gray-800 text-[11px] text-gray-500 flex flex-col gap-1 shrink-0">
          <div>{runtimeLabel}{runtimeTarget ? ` · ${runtimeTarget}` : ''}</div>
          <div>{workerLabel}</div>
        </div>

        {/* Log lines */}
        <div ref={scrollRef} onScroll={handleScroll} className="flex-1 overflow-y-auto p-3 font-mono text-xs">
          {lines.map((l, i) => {
            const lineNum = i + 1
            const selected = isSelected(lineNum)
            return (
              <div
                key={i}
                ref={(el) => { lineRefs.current[i] = el }}
                onClick={() => handleLineClick(lineNum)}
                className={`flex gap-2 leading-5 cursor-pointer rounded px-1 -mx-1 ${
                  selected ? 'bg-yellow-500/20' : 'hover:bg-gray-800'
                }`}
              >
                <span className="text-gray-600 shrink-0 select-none w-8 text-right">
                  {lineNum}
                </span>
                <span className="text-gray-600 shrink-0 select-none">
                  {new Date(l.logged_at).toLocaleTimeString()}
                </span>
                <span className={`whitespace-pre ${l.stream === 'stderr' ? 'text-orange-400' : 'text-gray-300'}`}>
                  {l.body}
                </span>
              </div>
            )
          })}
          {lines.length === 0 && (
            <p className="text-gray-600 mt-2">No logs yet.</p>
          )}
        </div>

        {/* Selected line indicator */}
        {selectedRange && (
          <div className="px-4 py-2 border-t border-gray-700 text-xs text-gray-500 shrink-0 flex items-center justify-between">
            <span>
              {selectedRange[0] === selectedRange[1]
                ? `Line ${selectedRange[0]}`
                : `Lines ${selectedRange[0]}–${selectedRange[1]}`}
            </span>
            <button
              onClick={() => navigate({ hash: '' }, { replace: true })}
              className="text-gray-600 hover:text-gray-400"
            >
              clear
            </button>
          </div>
        )}
      </div>
    </div>
  )
}
