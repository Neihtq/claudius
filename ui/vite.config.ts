import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

const BACKEND = 'http://localhost:8000'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  base: '/ui/',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
  server: {
    proxy: {
      '/sessions': { target: BACKEND, changeOrigin: true },
      '/workflows': { target: BACKEND, changeOrigin: true },
      '/dev':       { target: BACKEND, changeOrigin: true },
      '/webhook':   { target: BACKEND, changeOrigin: true },
      '/health':    { target: BACKEND, changeOrigin: true },
      '/executions': { target: BACKEND, changeOrigin: true },
    },
  },
})
