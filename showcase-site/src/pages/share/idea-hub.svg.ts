import { loadShowcase } from '../../lib/showcase-data.mjs';
import { shareSvg } from '../../lib/share-image.mjs';
export const prerender = true;
export function GET() {
  const { view } = loadShowcase();
  return new Response(shareSvg(view.title, view.subtitle), { headers: { 'Content-Type': 'image/svg+xml' } });
}
