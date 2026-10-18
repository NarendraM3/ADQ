import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import { loadEnv } from 'vite';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, '.', '');
  return {
    plugins: [react(), tailwindcss()],
    server: env.VITE_API_PROXY_TARGET ? { proxy: { '/api': env.VITE_API_PROXY_TARGET } } : undefined,
  };
});
