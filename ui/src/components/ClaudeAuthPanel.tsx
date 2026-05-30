import { useEffect, useState } from 'react'
import {
  exchangeOauth,
  getOauthStatus,
  logoutOauth,
  startOauth,
  type OAuthStatus,
} from '../api'

// Shown only when the controller runs with the `oauth` upstream auth mode. Drives
// the copy-paste PKCE login for the shared Claude subscription account.
export default function ClaudeAuthPanel() {
  const [status, setStatus] = useState<OAuthStatus | null>(null)
  const [enabled, setEnabled] = useState(true)
  const [busy, setBusy] = useState(false)
  const [authorizeUrl, setAuthorizeUrl] = useState<string | null>(null)
  const [code, setCode] = useState('')
  const [error, setError] = useState<string | null>(null)

  const refresh = () =>
    getOauthStatus()
      .then((s) => {
        setEnabled(s !== null)
        setStatus(s)
      })
      .catch((e) => setError(String(e?.message ?? e)))

  useEffect(() => {
    refresh()
  }, [])

  if (!enabled) return null

  const connected = !!status?.connected
  const expiresLabel =
    status?.expires_at != null ? new Date(status.expires_at * 1000).toLocaleString() : null

  const begin = async () => {
    setError(null)
    setBusy(true)
    try {
      const { authorize_url } = await startOauth()
      setAuthorizeUrl(authorize_url)
      window.open(authorize_url, '_blank', 'noopener')
    } catch (e: any) {
      setError(String(e?.message ?? e))
    } finally {
      setBusy(false)
    }
  }

  const submit = async () => {
    setError(null)
    setBusy(true)
    try {
      const s = await exchangeOauth(code.trim())
      setStatus(s)
      setAuthorizeUrl(null)
      setCode('')
    } catch (e: any) {
      setError(String(e?.message ?? e))
    } finally {
      setBusy(false)
    }
  }

  const disconnect = async () => {
    setError(null)
    setBusy(true)
    try {
      setStatus(await logoutOauth())
    } catch (e: any) {
      setError(String(e?.message ?? e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="mb-6 rounded border border-gray-800 bg-gray-900/40 p-4 text-sm">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="font-medium text-gray-100">Claude account</span>
          {connected ? (
            <span className={status?.expired ? 'text-orange-400' : 'text-green-400'}>
              ● {status?.expired ? 'token expired' : 'connected'}
            </span>
          ) : (
            <span className="text-gray-500">● not connected</span>
          )}
        </div>
        {connected ? (
          <button
            onClick={disconnect}
            disabled={busy}
            className="rounded px-3 py-1.5 text-xs text-gray-300 hover:bg-gray-800 disabled:opacity-50"
          >
            Disconnect
          </button>
        ) : (
          <button
            onClick={begin}
            disabled={busy}
            className="rounded bg-indigo-700 px-3 py-1.5 text-xs text-white hover:bg-indigo-600 disabled:opacity-50"
          >
            Log in with Claude
          </button>
        )}
      </div>

      {connected && (expiresLabel || status?.scope) && (
        <div className="mt-1 text-xs text-gray-500">
          {expiresLabel && <span>expires {expiresLabel}</span>}
          {status?.scope && <span className="ml-2">scope: {status.scope}</span>}
        </div>
      )}

      {!connected && authorizeUrl && (
        <div className="mt-3 space-y-2">
          <p className="text-xs text-gray-400">
            A Claude authorization page opened in a new tab. Approve access, then paste the
            code it shows (the <code className="text-gray-300">code#state</code> value) below.
          </p>
          <div className="flex gap-2">
            <input
              value={code}
              onChange={(e) => setCode(e.target.value)}
              placeholder="paste authorization code"
              className="flex-1 rounded border border-gray-700 bg-gray-950 px-2 py-1.5 text-xs text-gray-200 outline-none focus:border-gray-500"
            />
            <button
              onClick={submit}
              disabled={busy || !code.trim()}
              className="rounded bg-green-700 px-3 py-1.5 text-xs text-white hover:bg-green-600 disabled:opacity-50"
            >
              Connect
            </button>
          </div>
          <p className="text-xs text-gray-600">
            Didn't get a tab? <a className="underline" href={authorizeUrl} target="_blank" rel="noreferrer">Open the authorization page</a>.
          </p>
        </div>
      )}

      {error && <div className="mt-2 text-xs text-red-400">{error}</div>}
    </div>
  )
}
