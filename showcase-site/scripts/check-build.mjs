import { createHash } from 'node:crypto';
import { readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';

const outDir = resolve(process.env.SHOWCASE_OUT_DIR || './dist');
const slug = String(process.env.SHOWCASE_SLUG || '').trim();
const forbidden = [
  'github.com/onedayonemasterpiece/idea-hub',
  'inbox/voice/',
  'source_ref',
  'source_idea_ids',
];

function walk(root) {
  return readdirSync(root, { withFileTypes: true }).flatMap((entry) => {
    const full = join(root, entry.name);
    return entry.isDirectory() ? walk(full) : [full];
  });
}

const files = walk(outDir).sort();
const htmlFiles = files.filter((file) => file.endsWith('.html'));
if (!htmlFiles.length) throw new Error('Showcase build produced no HTML');
for (const file of htmlFiles) {
  const html = readFileSync(file, 'utf8');
  if (!html.includes('noindex') || !html.includes('noarchive') || !html.includes('no-referrer')) {
    throw new Error(`Missing private-publication metadata: ${relative(outDir, file)}`);
  }
  if (!html.includes(`/v/${slug}`)) {
    throw new Error(`Secret base path is absent: ${relative(outDir, file)}`);
  }
  for (const marker of forbidden) {
    if (html.includes(marker)) throw new Error(`Forbidden marker ${marker}: ${relative(outDir, file)}`);
  }
}

const inventory = files.map((file) => {
  const bytes = readFileSync(file);
  return {
    path: relative(outDir, file).replaceAll('\\', '/'),
    bytes: statSync(file).size,
    sha256: createHash('sha256').update(bytes).digest('hex'),
  };
});
const treeHash = createHash('sha256')
  .update(inventory.map((item) => `${item.path}\0${item.sha256}\n`).join(''))
  .digest('hex');
const headers = Object.fromEntries(htmlFiles.map((file) => [
  relative(outDir, file).replaceAll('\\', '/'),
  {
    'X-Robots-Tag': 'noindex, nofollow, noarchive, nosnippet, noimageindex',
    'Referrer-Policy': 'no-referrer',
    'Cache-Control': 'private, no-store',
  },
]));
writeFileSync(join(outDir, 'showcase-headers.json'), `${JSON.stringify(headers, null, 2)}\n`);
writeFileSync(join(outDir, 'showcase-build.json'), `${JSON.stringify({
  schema_version: 1,
  checked_at: new Date().toISOString(),
  file_count: inventory.length + 2,
  html_count: htmlFiles.length,
  tree_sha256: treeHash,
  files: inventory,
}, null, 2)}\n`);
console.log(`Showcase build checked: ${htmlFiles.length} HTML pages, tree=${treeHash}`);
