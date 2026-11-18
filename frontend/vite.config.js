import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
// The production build is written straight into the Python package so
// `pytaskq dashboard` serves it at "/" with no extra steps.
// `npm run dev` serves the dashboard on :5173 and forwards API calls to the
// PyTaskQ server on :8000, so the browser sees one origin (cookies just work).
const API = 'http://localhost:8000';
const proxy = Object.fromEntries(
  ['/auth', '/task', '/tasks', '/metrics', '/system', '/dlq', '/health'].map(p => [p, API]),
);
proxy['/ws'] = { target: API, ws: true };

export default defineConfig({
  plugins: [react()],
  base: '/static/',
  server: { proxy },
  build: {
    outDir: '../pytaskq/static',
    emptyOutDir: true,
  },
})
