import { useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { devInject, listWorkflows } from '../api'
import type { Attachment } from '../types'

interface Props {
  onClose: () => void
}

function readAsBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => resolve((reader.result as string).split(',')[1])
    reader.onerror = reject
    reader.readAsDataURL(file)
  })
}

export default function NewSessionModal({ onClose }: Props) {
  const navigate = useNavigate()
  const [workflows, setWorkflows] = useState<string[]>([])
  const [workflow, setWorkflow] = useState('')
  const [channel, setChannel] = useState('email')
  const [sender, setSender] = useState('dev@localhost')
  const [subject, setSubject] = useState('')
  const [phone, setPhone] = useState('')
  const [body, setBody] = useState('')
  const [attachments, setAttachments] = useState<Attachment[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const fileRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    listWorkflows()
      .then((wfs) => {
        setWorkflows(wfs)
        if (wfs.length) setWorkflow(wfs[0])
      })
      .catch(console.error)
  }, [])

  const handleFiles = async (files: FileList | null) => {
    if (!files) return
    const result: Attachment[] = []
    for (const f of Array.from(files)) {
      const data = await readAsBase64(f)
      result.push({ filename: f.name, content_type: f.type || 'application/octet-stream', data })
    }
    setAttachments((prev) => [...prev, ...result])
  }

  const handleSubmit = async () => {
    if (!body.trim() || !workflow) return
    setLoading(true)
    setError('')
    try {
      const { session_id } = await devInject({
        channel,
        sender: channel === 'whatsapp' ? phone : sender,
        subject: channel === 'email' ? subject : undefined,
        body,
        attachments,
      })
      navigate(`/sessions/${session_id}`)
    } catch (e) {
      setError(String(e))
      setLoading(false)
    }
  }

  return (
    <div
      className="fixed inset-0 bg-black/60 flex items-center justify-center z-50"
      onClick={onClose}
    >
      <div
        className="bg-gray-900 border border-gray-700 rounded-lg p-6 w-full max-w-md"
        onClick={(e) => e.stopPropagation()}
      >
        <h2 className="text-gray-100 font-semibold mb-4">New Session</h2>

        <div className="grid grid-cols-2 gap-3 mb-3">
          <label className="block">
            <span className="text-gray-500 text-xs uppercase tracking-wide">Channel</span>
            <select
              value={channel}
              onChange={(e) => setChannel(e.target.value)}
              className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-sm text-gray-200"
            >
              <option value="email">Email</option>
              <option value="whatsapp">WhatsApp</option>
            </select>
          </label>
          <label className="block">
            <span className="text-gray-500 text-xs uppercase tracking-wide">Workflow</span>
            <select
              value={workflow}
              onChange={(e) => setWorkflow(e.target.value)}
              className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-sm text-gray-200"
            >
              {workflows.map((w) => (
                <option key={w} value={w}>
                  {w}
                </option>
              ))}
            </select>
          </label>
        </div>

        {channel === 'email' && (
          <div className="grid grid-cols-2 gap-3 mb-3">
            <label className="block">
              <span className="text-gray-500 text-xs uppercase tracking-wide">From</span>
              <input
                value={sender}
                onChange={(e) => setSender(e.target.value)}
                className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-sm text-gray-200"
              />
            </label>
            <label className="block">
              <span className="text-gray-500 text-xs uppercase tracking-wide">Subject</span>
              <input
                value={subject}
                onChange={(e) => setSubject(e.target.value)}
                className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-sm text-gray-200"
              />
            </label>
          </div>
        )}

        {channel === 'whatsapp' && (
          <label className="block mb-3">
            <span className="text-gray-500 text-xs uppercase tracking-wide">Phone number</span>
            <input
              value={phone}
              onChange={(e) => setPhone(e.target.value)}
              placeholder="+1 555 000 0000"
              className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-sm text-gray-200"
            />
          </label>
        )}

        <label className="block mb-3">
          <span className="text-gray-500 text-xs uppercase tracking-wide">Message</span>
          <textarea
            value={body}
            onChange={(e) => setBody(e.target.value)}
            rows={4}
            className="mt-1 w-full bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-sm text-gray-200 resize-none"
          />
        </label>

        {error && <p className="text-red-400 text-xs mb-3">{error}</p>}

        <div className="flex items-center gap-3">
          <button
            onClick={() => fileRef.current?.click()}
            className="bg-gray-800 border border-gray-700 text-gray-400 px-3 py-1.5 rounded text-sm"
          >
            📎 {attachments.length > 0 ? `${attachments.length} file(s)` : 'Attach'}
          </button>
          <input
            ref={fileRef}
            type="file"
            multiple
            className="hidden"
            onChange={(e) => handleFiles(e.target.files)}
          />
          <div className="flex-1" />
          <button onClick={onClose} className="text-gray-500 text-sm px-3 py-1.5">
            Cancel
          </button>
          <button
            onClick={handleSubmit}
            disabled={loading || !body.trim() || !workflow}
            className="bg-green-700 hover:bg-green-600 disabled:opacity-50 text-white px-4 py-2 rounded text-sm"
          >
            {loading ? 'Starting…' : 'Start Session'}
          </button>
        </div>
      </div>
    </div>
  )
}
