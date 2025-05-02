import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: 'dist',                // читается в build.gradle.kts
    emptyOutDir: true
  },
  server: {
    proxy: {
      '/api': 'http://localhost:8081',   // ваш Javalin
    },
  },
});
