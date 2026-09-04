import { loadShowcase } from '../../../lib/showcase-data.mjs';
import { sharePng } from '../../../lib/share-image.mjs';
export const prerender = true;
export function getStaticPaths() { return loadShowcase().items.map((item) => ({ params: { id: item.id }, props: { item } })); }
export async function GET({ props }) {
  return new Response(new Uint8Array(await sharePng(props.item.title, props.item.summary)), { headers: { 'Content-Type': 'image/png' } });
}
