import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react()],
  base: './',
  build: {
    assetsDir: 'assets',
    rollupOptions: {
      output: {
        entryFileNames: 'assets/report-ui.js',
        chunkFileNames: 'assets/report-ui-[name].js',
        assetFileNames: 'assets/report-ui.[ext]'
      }
    }
  },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: []
  }
});
