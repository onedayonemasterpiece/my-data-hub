import { loadShowcase } from '../../lib/showcase-data.mjs';
import { sharePng } from '../../lib/share-image.mjs';
export const prerender = true;
export async function GET() {
  const { view } = loadShowcase();
  return new Response(new Uint8Array(await sharePng(view.title, view.subtitle)), { headers: { 'Content-Type': 'image/png' } });
}
