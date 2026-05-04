import { Fragment, useEffect, useMemo, useState } from 'react'
import { getProxyLogs, subscribeToSession } from '../api'
import type { Execution, ProxyLog, SSEEvent } from '../types'

interface Props {
  execution: Execution
  onClose: () => void
}

const STAGE_LABEL: Record<ProxyLog['stage'], string> = {
  request_in: 'Worker -> Proxy',
  request_out: 'Proxy -> Upstream',
  response_in: 'Upstream -> Proxy',
  response_out: 'Proxy -> Worker',
}

interface JsonToken {
  text: string
  className: string
}

interface ProxyExchange {
  id: string
  method: string | null
  path: string | null
  startedAt: string
  entries: Array<ProxyLog & { rowId: string }>
}

interface UsageTotals {
  input_tokens: number
  output_tokens: number
  cache_creation_input_tokens: number
  cache_read_input_tokens: number
  total_tokens: number
  total_cost_usd: number
}

function isJsonEntry(entry: ProxyLog): boolean {
  return entry.content_type?.toLowerCase().includes('application/json') ?? false
}

function tokenizeJson(value: unknown, indent = 0): JsonToken[] {
  const spacing = '  '.repeat(indent)
  if (value === null) return [{ text: 'null', className: 'text-red-400' }]
  if (typeof value === 'string') {
    return [{ text: JSON.stringify(value), className: 'text-emerald-300' }]
  }
  if (typeof value === 'number') {
    return [{ text: String(value), className: 'text-amber-300' }]
  }
  if (typeof value === 'boolean') {
    return [{ text: String(value), className: 'text-violet-300' }]
  }
  if (Array.isArray(value)) {
    if (value.length === 0) return [{ text: '[]', className: 'text-gray-300' }]
    const tokens: JsonToken[] = [{ text: '[\n', className: 'text-gray-300' }]
    value.forEach((item, index) => {
      tokens.push({ text: `${'  '.repeat(indent + 1)}`, className: 'text-gray-300' })
      tokens.push(...tokenizeJson(item, indent + 1))
      tokens.push({ text: index < value.length - 1 ? ',\n' : '\n', className: 'text-gray-300' })
    })
    tokens.push({ text: `${spacing}]`, className: 'text-gray-300' })
    return tokens
  }
  const entries = Object.entries(value as Record<string, unknown>)
  if (entries.length === 0) return [{ text: '{}', className: 'text-gray-300' }]
  const tokens: JsonToken[] = [{ text: '{\n', className: 'text-gray-300' }]
  entries.forEach(([key, entryValue], index) => {
    tokens.push({ text: `${'  '.repeat(indent + 1)}${JSON.stringify(key)}: `, className: 'text-sky-300' })
    tokens.push(...tokenizeJson(entryValue, indent + 1))
    tokens.push({ text: index < entries.length - 1 ? ',\n' : '\n', className: 'text-gray-300' })
  })
  tokens.push({ text: `${spacing}}`, className: 'text-gray-300' })
  return tokens
}

function renderJson(body: string) {
  try {
    const parsed = JSON.parse(body)
    return tokenizeJson(parsed).map((token, index) => (
      <span key={index} className={token.className}>
        {token.text}
      </span>
    ))
  } catch {
    return body
  }
}

function formatCost(totalCostUsd: number | null): string | null {
  return totalCostUsd ? `$${totalCostUsd.toFixed(4)}` : null
}

function buildUsageSummary(entry: ProxyLog): string {
  const parts = [
    entry.total_tokens ? `${entry.total_tokens.toLocaleString()} tokens` : null,
    formatCost(entry.total_cost_usd),
  ].filter(Boolean)
  return parts.join(' · ')
}

function buildUsageBreakdown(entry: ProxyLog): string {
  const parts = [
    entry.input_tokens ? `in ${entry.input_tokens}` : null,
    entry.output_tokens ? `out ${entry.output_tokens}` : null,
    entry.cache_creation_input_tokens ? `cache create ${entry.cache_creation_input_tokens}` : null,
    entry.cache_read_input_tokens ? `cache read ${entry.cache_read_input_tokens}` : null,
  ].filter(Boolean)
  return parts.join(' · ')
}

function sumUsage(entries: ProxyLog[]): UsageTotals | null {
  const totals = entries.reduce<UsageTotals>(
    (acc, entry) => {
      acc.input_tokens += entry.input_tokens ?? 0
      acc.output_tokens += entry.output_tokens ?? 0
      acc.cache_creation_input_tokens += entry.cache_creation_input_tokens ?? 0
      acc.cache_read_input_tokens += entry.cache_read_input_tokens ?? 0
      acc.total_tokens += entry.total_tokens ?? 0
      acc.total_cost_usd += entry.total_cost_usd ?? 0
      return acc
    },
    {
      input_tokens: 0,
      output_tokens: 0,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
      total_tokens: 0,
      total_cost_usd: 0,
    },
  )

  const hasUsage =
    totals.input_tokens > 0 ||
    totals.output_tokens > 0 ||
    totals.cache_creation_input_tokens > 0 ||
    totals.cache_read_input_tokens > 0 ||
    totals.total_tokens > 0 ||
    totals.total_cost_usd > 0

  return hasUsage ? totals : null
}

function buildUsageSummaryFromTotals(totals: UsageTotals | null): string {
  if (!totals) return ''
  const parts = [
    totals.total_tokens ? `${totals.total_tokens.toLocaleString()} tokens` : null,
    formatCost(totals.total_cost_usd),
  ].filter(Boolean)
  return parts.join(' · ')
}

function buildUsageBreakdownFromTotals(totals: UsageTotals | null): string {
  if (!totals) return ''
  const parts = [
    totals.input_tokens ? `in ${totals.input_tokens}` : null,
    totals.output_tokens ? `out ${totals.output_tokens}` : null,
    totals.cache_creation_input_tokens ? `cache create ${totals.cache_creation_input_tokens}` : null,
    totals.cache_read_input_tokens ? `cache read ${totals.cache_read_input_tokens}` : null,
  ].filter(Boolean)
  return parts.join(' · ')
}

function buildExchanges(entries: ProxyLog[]): ProxyExchange[] {
  const exchanges: ProxyExchange[] = []
  const byRequestId = new Map<string, ProxyExchange>()
  let legacyCurrent: ProxyExchange | null = null

  entries.forEach((entry, index) => {
    const rid = entry.request_id

    if (rid) {
      let exchange = byRequestId.get(rid)
      if (!exchange) {
        exchange = {
          id: rid,
          method: entry.method,
          path: entry.path,
          startedAt: entry.logged_at,
          entries: [],
        }
        byRequestId.set(rid, exchange)
        exchanges.push(exchange)
      }
      exchange.entries.push({ ...entry, rowId: `${rid}-${entry.stage}` })
      if (!exchange.method && entry.method) exchange.method = entry.method
      if (!exchange.path && entry.path) exchange.path = entry.path
      return
    }

    // Fallback for legacy rows without request_id
    if (entry.stage === 'request_in' || legacyCurrent === null) {
      legacyCurrent = {
        id: `${entry.logged_at}-${index}`,
        method: entry.method,
        path: entry.path,
        startedAt: entry.logged_at,
        entries: [],
      }
      exchanges.push(legacyCurrent)
    }
    legacyCurrent.entries.push({ ...entry, rowId: `${entry.logged_at}-${index}-${entry.stage}` })
    if (!legacyCurrent.method && entry.method) legacyCurrent.method = entry.method
    if (!legacyCurrent.path && entry.path) legacyCurrent.path = entry.path
  })

  return exchanges
}

export default function ProxyLogPanel({ execution, onClose }: Props) {
  const [entries, setEntries] = useState<ProxyLog[]>([])
  const [openExchanges, setOpenExchanges] = useState<string[]>([])
  const [openRows, setOpenRows] = useState<string[]>([])

  useEffect(() => {
    const buffered: ProxyLog[] = []
    let fetched = false

    const unsub = subscribeToSession(execution.session_id, (event: SSEEvent) => {
      if (event.type !== 'proxy_log' || event.execution_id !== execution.execution_id) return
      const entry: ProxyLog = {
        logged_at: event.logged_at,
        stage: event.stage,
        method: event.method,
        path: event.path,
        upstream_url: event.upstream_url,
        status_code: event.status_code,
        content_type: event.content_type,
        body: event.body,
        meta: event.meta,
        input_tokens: event.input_tokens,
        output_tokens: event.output_tokens,
        cache_creation_input_tokens: event.cache_creation_input_tokens,
        cache_read_input_tokens: event.cache_read_input_tokens,
        total_tokens: event.total_tokens,
        total_cost_usd: event.total_cost_usd,
        request_id: event.request_id,
      }
      if (fetched) {
        setEntries((prev) => [...prev, entry])
      } else {
        buffered.push(entry)
      }
    })

    getProxyLogs(execution.execution_id)
      .then((historical) => {
        fetched = true
        const all = [...historical, ...buffered]
        const seen = new Set<string>()
        const deduped = all.filter((entry) => {
          const key = JSON.stringify(entry)
          if (seen.has(key)) return false
          seen.add(key)
          return true
        })
        deduped.sort((a, b) => a.logged_at.localeCompare(b.logged_at))
        setEntries(deduped)
      })
      .catch(console.error)

    return unsub
  }, [execution.execution_id, execution.session_id])

  useEffect(() => {
    const validRows = new Set(entries.map((entry, index) => `${entry.logged_at}-${index}-${entry.stage}`))
    setOpenRows((prev) => prev.filter((rowId) => validRows.has(rowId)))
  }, [entries])

  const exchanges = useMemo(() => buildExchanges(entries), [entries])
  const executionUsageTotals = useMemo(
    () =>
      sumUsage([
        {
          logged_at: execution.started_at,
          stage: 'response_in',
          method: null,
          path: null,
          upstream_url: null,
          status_code: null,
          content_type: null,
          body: '',
          meta: {},
          input_tokens: execution.claude_input_tokens,
          output_tokens: execution.claude_output_tokens,
          cache_creation_input_tokens: execution.claude_cache_creation_input_tokens,
          cache_read_input_tokens: execution.claude_cache_read_input_tokens,
          total_tokens: execution.claude_total_tokens,
          total_cost_usd: execution.claude_total_cost_usd,
          request_id: null,
        },
      ]),
    [
      execution.started_at,
      execution.claude_input_tokens,
      execution.claude_output_tokens,
      execution.claude_cache_creation_input_tokens,
      execution.claude_cache_read_input_tokens,
      execution.claude_total_tokens,
      execution.claude_total_cost_usd,
    ],
  )
  const executionUsageSummary = buildUsageSummaryFromTotals(executionUsageTotals)
  const executionUsageBreakdown = buildUsageBreakdownFromTotals(executionUsageTotals)

  useEffect(() => {
    const validExchanges = new Set(exchanges.map((exchange) => exchange.id))
    setOpenExchanges((prev) => prev.filter((exchangeId) => validExchanges.has(exchangeId)))
  }, [exchanges])

  const toggleExchange = (exchangeId: string) => {
    setOpenExchanges((prev) =>
      prev.includes(exchangeId)
        ? prev.filter((value) => value !== exchangeId)
        : [...prev, exchangeId],
    )
  }

  const toggleRow = (rowId: string) => {
    setOpenRows((prev) =>
      prev.includes(rowId) ? prev.filter((value) => value !== rowId) : [...prev, rowId],
    )
  }

  return (
    <div className="fixed inset-0 z-50 flex justify-end" onClick={(e) => e.target === e.currentTarget && onClose()}>
      <div className="w-[560px] max-w-full h-full bg-gray-900 border-l border-gray-700 flex flex-col shadow-2xl">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700 shrink-0">
          <div className="flex items-center gap-3">
            <span className="text-xs text-gray-400 font-mono">
              proxy {execution.execution_id.slice(0, 8)}
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
        {(executionUsageSummary || executionUsageBreakdown) && (
          <div className="px-4 py-2 border-b border-gray-800 text-[11px] text-gray-500 flex flex-col gap-1 shrink-0">
            {executionUsageSummary && <div>execution usage: <span className="text-gray-300 font-mono">{executionUsageSummary}</span></div>}
            {executionUsageBreakdown && <div>breakdown: <span className="text-gray-400 font-mono">{executionUsageBreakdown}</span></div>}
          </div>
        )}

        <div className="flex-1 overflow-y-auto p-3 flex flex-col gap-2">
          {exchanges.map((exchange, exchangeIndex) => {
            const exchangeOpen = openExchanges.includes(exchange.id)
            const exchangeTotals = sumUsage(exchange.entries)
            const exchangeUsageSummary = buildUsageSummaryFromTotals(exchangeTotals)
            const exchangeUsageBreakdown = buildUsageBreakdownFromTotals(exchangeTotals)
            return (
              <div key={exchange.id} className="border border-gray-800 rounded-lg overflow-hidden bg-gray-950">
                <button
                  type="button"
                  onClick={() => toggleExchange(exchange.id)}
                  className="w-full px-4 py-3 border-b border-gray-800 bg-gray-900/70 hover:bg-gray-900 text-left"
                >
                  <div className="flex items-center gap-3 text-xs">
                    <span className="text-gray-500 w-4">{exchangeOpen ? '▾' : '▸'}</span>
                    <span className="text-blue-400">Exchange {exchangeIndex + 1}</span>
                    <span className="text-gray-400 font-mono">
                      {exchange.method ?? 'UNKNOWN'} {exchange.path ?? ''}
                    </span>
                    {exchangeUsageSummary && (
                      <span className="text-gray-500 font-mono">
                        {exchangeUsageSummary}
                      </span>
                    )}
                    <span className="text-gray-500 ml-auto">
                      {new Date(exchange.startedAt).toLocaleTimeString()}
                    </span>
                  </div>
                  {exchangeUsageBreakdown && (
                    <div className="mt-1 pl-7 text-[11px] text-gray-500 font-mono">
                      {exchangeUsageBreakdown}
                    </div>
                  )}
                </button>

                {exchangeOpen && (
                  <table className="w-full table-fixed text-xs">
                    <thead className="bg-gray-950/80 text-gray-500">
                      <tr className="border-b border-gray-800">
                        <th className="w-36 px-4 py-2 text-left font-normal">Stage</th>
                        <th className="w-28 px-4 py-2 text-left font-normal">Time</th>
                        <th className="px-4 py-2 text-left font-normal">Method</th>
                        <th className="w-20 px-4 py-2 text-left font-normal">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {exchange.entries.map((entry) => {
                        const isOpen = openRows.includes(entry.rowId)
                        const usageSummary = buildUsageSummary(entry)
                        const usageBreakdown = buildUsageBreakdown(entry)
                        return (
                          <Fragment key={entry.rowId}>
                            <tr
                              onClick={() => toggleRow(entry.rowId)}
                              className="cursor-pointer border-b border-gray-800/70 hover:bg-gray-900/70"
                            >
                              <td className="px-4 py-3 text-blue-400">{STAGE_LABEL[entry.stage]}</td>
                              <td className="px-4 py-3 text-gray-500 whitespace-nowrap">
                                {new Date(entry.logged_at).toLocaleTimeString()}
                              </td>
                              <td className="px-4 py-3 text-gray-400 font-mono">
                                <div className="truncate">{entry.method ?? '—'}</div>
                              </td>
                              <td className="px-4 py-3 whitespace-nowrap">
                                {entry.status_code !== null ? (
                                  <span className={entry.status_code >= 400 ? 'text-red-400' : 'text-green-400'}>
                                    {entry.status_code}
                                  </span>
                                ) : (
                                  <span className="text-gray-600">—</span>
                                )}
                              </td>
                            </tr>
                            {isOpen && (
                              <tr className="border-b border-gray-800/70 bg-gray-900/40">
                                <td colSpan={4} className="px-4 py-3">
                                  <div className="min-w-0">
                                    <div className="text-xs text-gray-500 flex flex-col gap-1 mb-3">
                                      {entry.path && (
                                        <div>
                                          path: <span className="text-gray-400 font-mono break-all">{entry.path}</span>
                                        </div>
                                      )}
                                      {entry.upstream_url && (
                                        <div>
                                          upstream: <span className="text-gray-400 font-mono break-all">{entry.upstream_url}</span>
                                        </div>
                                      )}
                                      {entry.content_type && (
                                        <div>
                                          content-type: <span className="text-gray-400 font-mono">{entry.content_type}</span>
                                        </div>
                                      )}
                                      {usageSummary && (
                                        <div>
                                          usage: <span className="text-gray-400 font-mono">{usageSummary}</span>
                                        </div>
                                      )}
                                      {usageBreakdown && (
                                        <div>
                                          breakdown: <span className="text-gray-400 font-mono">{usageBreakdown}</span>
                                        </div>
                                      )}
                                      {Object.keys(entry.meta).length > 0 && (
                                        <div>
                                          meta: <span className="text-gray-400 font-mono break-all">{JSON.stringify(entry.meta)}</span>
                                        </div>
                                      )}
                                    </div>
                                    <pre className="m-0 max-h-96 overflow-auto rounded border border-gray-800 bg-gray-950 p-4 text-xs whitespace-pre-wrap break-words text-gray-200">
                                      {entry.body.trim()
                                        ? isJsonEntry(entry)
                                          ? renderJson(entry.body)
                                          : entry.body
                                        : '[empty body]'}
                                    </pre>
                                  </div>
                                </td>
                              </tr>
                            )}
                          </Fragment>
                        )
                      })}
                    </tbody>
                  </table>
                )}
              </div>
            )
          })}
          {exchanges.length === 0 && (
            <p className="text-gray-600 mt-2">No proxy messages yet.</p>
          )}
        </div>
      </div>
    </div>
  )
}
