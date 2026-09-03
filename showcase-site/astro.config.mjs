// @ts-check
import { defineConfig } from 'astro/config';
import { resolve } from 'node:path';

const slug = String(process.env.SHOWCASE_SLUG || '').trim();
if (!/^[A-Za-z0-9_-]{20,80}$/u.test(slug)) {
  throw new Error('SHOWCASE_SLUG must be a 20-80 character URL-safe secret');
}

const origin = String(process.env.SHOWCASE_ORIGIN || 'https://ideas.kenigevents.ru').replace(/\/+$/u, '');
const outDir = resolve(process.env.SHOWCASE_OUT_DIR || './dist');
const cacheRoot = resolve(process.env.SHOWCASE_CACHE_DIR || './.cache');

export default defineConfig({
  site: origin,
  base: `/v/${slug}`,
  output: 'static',
  trailingSlash: 'always',
  outDir,
  cacheDir: resolve(cacheRoot, 'astro'),
  build: {
    assets: '_assets',
  },
  vite: {
    cacheDir: resolve(cacheRoot, 'vite'),
    server: {
      allowedHosts: true,
    },
  },
});
