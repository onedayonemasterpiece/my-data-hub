import { loadShowcase } from '../../lib/showcase-data.mjs';
import { shareSvg } from '../../lib/share-image.mjs';
export const prerender = true;
export function getStaticPaths() { return loadShowcase().items.map((item) => ({ params: { id: item.id }, props: { item } })); }
export function GET({ props }) {
  return new Response(shareSvg(props.item.title, props.item.summary), { headers: { 'Content-Type': 'image/svg+xml' } });
}
