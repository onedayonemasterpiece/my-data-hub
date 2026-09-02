import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const inputPath = resolve(process.env.SHOWCASE_INPUT || './showcase-input.json');
let cached;

export function loadShowcase() {
  if (cached) return cached;
  const parsed = JSON.parse(readFileSync(inputPath, 'utf8'));
  if (parsed?.schema_version !== 1 || !parsed?.view || !Array.isArray(parsed?.items)) {
    throw new Error('Invalid showcase input');
  }
  cached = parsed;
  return cached;
}
