import { Navigate, Route, Routes } from 'react-router-dom'
import Conversation from './Conversation'
import SessionsList from './SessionsList'

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Navigate to="/sessions" replace />} />
      <Route path="/sessions" element={<SessionsList />} />
      <Route path="/sessions/:sessionId" element={<Conversation />} />
      <Route path="/sessions/:sessionId/logs/:executionId" element={<Conversation />} />
      <Route path="/sessions/:sessionId/proxy/:proxyExecutionId" element={<Conversation />} />
      <Route path="/sessions/:sessionId/conversation/:conversationExecutionId" element={<Conversation />} />
    </Routes>
  )
}
