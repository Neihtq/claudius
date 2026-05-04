import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { listSessions } from '../api'
import type { ClaudeSummary, Session } from '../types'
import NewSessionModal from './NewSessionModal'

const STATE_COLOR: Record<string, string> = {
  active: 'text-green-400',
  waiting: 'text-orange-400',
  hibernated: 'text-gray-500',
  closed: 'text-gray-500',
  new: 'text-blue-400',
  error: 'text-red-400',
}

function formatDuration(durationMs: number): string | null {
  if (!durationMs) return null
  if (durationMs < 60_000) return `${(durationMs / 1000).toFixed(1)}s`
  return `${(durationMs / 60_000).toFixed(1)}m`
}

function formatCost(totalCostUsd: number): string | null {
  return totalCostUsd ? `$${totalCostUsd.toFixed(4)}` : null
}

function buildSummary(summary: ClaudeSummary): string {
  const parts = [
    summary.num_turns ? `${summary.num_turns} turns` : null,
    formatDuration(summary.duration_ms),
    summary.total_tokens ? `${summary.total_tokens.toLocaleString()} tokens` : null,
    formatCost(summary.total_cost_usd),
  ].filter(Boolean)
  return parts.join(' · ')
}

export default function SessionsList() {
  const navigate = useNavigate()
  const [sessions, setSessions] = useState<Session[]>([])
  const [showModal, setShowModal] = useState(false)

  const load = () => listSessions().then(setSessions).catch(console.error)

  useEffect(() => {
    load()
    const t = setInterval(load, 5000)
    return () => clearInterval(t)
  }, [])

  return (
    <div className="min-h-screen bg-gray-950 text-gray-300 p-6">
      <div className="max-w-4xl mx-auto">
        <div className="flex items-center justify-between mb-6">
          <h1 className="text-xl font-semibold text-gray-100">Claudius</h1>
          <button
            onClick={() => setShowModal(true)}
            className="bg-green-700 hover:bg-green-600 text-white px-4 py-2 rounded text-sm"
          >
            + New Session
          </button>
        </div>

        <table className="w-full text-sm">
          <thead>
            <tr className="text-gray-500 text-left border-b border-gray-800">
              <th className="pb-2 pr-4 font-normal">Session</th>
              <th className="pb-2 pr-4 font-normal">Workflow</th>
              <th className="pb-2 pr-4 font-normal">Channel</th>
              <th className="pb-2 pr-4 font-normal">State</th>
              <th className="pb-2 pr-4 font-normal">Claude</th>
              <th className="pb-2 font-normal">Last message</th>
            </tr>
          </thead>
          <tbody>
            {sessions.map((s) => (
              <tr
                key={s.session_id}
                onClick={() => navigate(`/sessions/${s.session_id}`)}
                className="border-b border-gray-800/50 hover:bg-gray-900 cursor-pointer"
              >
                <td className="py-3 pr-4 font-mono text-xs text-gray-400">
                  {s.session_id.slice(0, 8)}
                </td>
                <td className="py-3 pr-4">{s.workflow_name}</td>
                <td className="py-3 pr-4">{s.channel}</td>
                <td className={`py-3 pr-4 ${STATE_COLOR[s.state] ?? 'text-gray-500'}`}>
                  {s.state}
                  {s.last_execution_result === 'failed' && (
                    <span className="ml-2 text-red-400" title="Last execution failed">⚠</span>
                  )}
                </td>
                <td className="py-3 pr-4 text-xs text-gray-500">
                  {buildSummary(s.claude_summary) || '—'}
                </td>
                <td className="py-3 text-gray-500 text-xs">
                  {new Date(s.last_message_at).toLocaleString()}
                </td>
              </tr>
            ))}
            {sessions.length === 0 && (
              <tr>
                <td colSpan={6} className="py-8 text-center text-gray-600">
                  No sessions yet — click "+ New Session" to start one.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {showModal && (
        <NewSessionModal onClose={() => setShowModal(false)} />
      )}
    </div>
  )
}
