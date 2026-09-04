import sharp from 'sharp';
const escape = (value) => String(value).replace(/[&<>"']/g, (char) => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&apos;' }[char]));
function lines(value, width, max) {
  const chunks = String(value).trim().split(/\s+/u).flatMap((word) => word.match(new RegExp(`.{1,${width}}`, 'gu')) || []);
  const result = [];
  for (const word of chunks) {
    if (!result.length || result[result.length - 1].length + word.length + 1 > width) result.push(word);
    else result[result.length - 1] += ` ${word}`;
  }
  if (result.length > max) { result.length = max; result[max - 1] = `${result[max - 1].slice(0, width - 1)}…`; }
  return result;
}
export function shareSvg(title, text) {
  const heading = lines(title, 36, 3);
  const subtitle = lines(text, 65, 3);
  return `<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630">
    <rect width="1200" height="630" fill="#f6f7fa"/><rect x="36" y="36" width="1128" height="558" rx="28" fill="#ffffff"/>
    <path d="M80 82h60" stroke="#175ac7" stroke-width="8" stroke-linecap="round"/>
    <text x="162" y="92" fill="#586376" font-family="DejaVu Sans, sans-serif" font-size="26">IdeaHub · Задачи и возможности</text>
    ${heading.map((line, i) => `<text x="80" y="${180+i*62}" fill="#182236" font-family="DejaVu Sans, sans-serif" font-size="48" font-weight="700">${escape(line)}</text>`).join('')}
    ${subtitle.map((line, i) => `<text x="80" y="${382+i*38}" fill="#586376" font-family="DejaVu Sans, sans-serif" font-size="27">${escape(line)}</text>`).join('')}
    <text x="80" y="550" fill="#175ac7" font-family="DejaVu Sans, sans-serif" font-size="24">От задачи — к понятному результату</text>
  </svg>`;
}
export async function sharePng(title, text) {
  return sharp(Buffer.from(shareSvg(title, text)), { limitInputPixels: 1200 * 630 }).png().toBuffer();
}
