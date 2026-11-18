import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
// The production build is written straight into the Python package so
// `pytaskq dashboard` serves it at "/" with no extra steps.
export default defineConfig({
  plugins: [react()],
  base: '/static/',
  build: {
    outDir: '../pytaskq/static',
    emptyOutDir: true,
  },
})
