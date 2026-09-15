#!/usr/bin/env python3
"""Prepare only spoken slide paragraphs; never pass headings or audit notes to TTS."""
from __future__ import annotations
import base64
import hashlib
import json
import re
import runpy
from pathlib import Path

HERE = Path(__file__).resolve().parent
OLD = HERE.parent / 'lecture-tts-20260915'
REF_SHA = 'bd92c7a4343e2e07256942bb036d7db9ef885eda5985109f140849ce081ccf5a'

def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def parse_slides(text: str) -> list[dict]:
    matches = list(re.finditer(r'^## Слайд (\d+)\. ([^\n]+)\n', text, re.M))
    if not matches or text[:matches[0].start()].strip():
        raise ValueError('Missing slide heading or unexpected preamble')
    slides = []
    for i, match in enumerate(matches):
        end = matches[i+1].start() if i+1 < len(matches) else len(text)
        body = text[match.end():end].strip()
        if len(body) < 80 or re.search(r'https?://|^#|\[\[|```', body, re.M):
            raise ValueError(f'Non-spoken markup or empty body: {match.group(1)}')
        slides.append({'number': int(match.group(1)), 'heading': f'Слайд {match.group(1)}. {match.group(2)}', 'paragraphs': [p.strip() for p in re.split(r'\n\s*\n', body) if p.strip()]})
    if [s['number'] for s in slides] != list(range(1, 30)):
        raise ValueError('Expected exactly slides 1..29, in order')
    return slides

def main() -> None:
    out = HERE / 'inputs'
    out.mkdir(parents=True, exist_ok=True)
    parts = [HERE / f'narration-{i:02d}.md' for i in range(1, 4)]
    text = '\n'.join(p.read_text(encoding='utf-8').strip() for p in parts) + '\n'
    slides = parse_slides(text)
    payload = {'revision': 'expanded-v2-20260915', 'title': 'Компьютер в кубе', 'slides': slides}
    data = (json.dumps(payload, ensure_ascii=False, indent=2) + '\n').encode('utf-8')
    (out / 'narration.json').write_bytes(data)
    (out / 'narration.md').write_text(text, encoding='utf-8')
    (out / 'narration-spoken-only.txt').write_text('\n\n\n'.join('\n\n'.join(s['paragraphs']) for s in slides) + '\n', encoding='utf-8')
    reference = base64.b64decode(''.join((OLD / f'reference.part{i}.b64').read_text(encoding='utf-8').strip() for i in range(1, 7)), validate=True)
    if digest(reference) != REF_SHA:
        raise ValueError('Reference checksum mismatch')
    (out / 'reference.ogg').write_bytes(reference)
    worker = runpy.run_path(str(OLD / 'longform_shard.py'))
    chunks = worker['chunk_slides'](payload)
    source_words = [w for s in slides for p in s['paragraphs'] for w in p.split()]
    chunk_words = [w for c in chunks for w in c['text'].split()]
    if source_words != chunk_words:
        raise ValueError('Chunking lost, duplicated or reordered text')
    if any(len(c['text'].split()) > 72 for c in chunks):
        raise ValueError('An oversized chunk requires an editorial split')
    if len(set(c['slide'] for c in chunks)) != 29:
        raise ValueError('A slide disappeared during chunking')
    # Negative tests: a missing or duplicate slide must fail closed.
    for broken in [text.replace('## Слайд 29.', '## Слайд 28.', 1), text.replace('## Слайд 1.', '## Слайд 30.', 1)]:
        try:
            parse_slides(broken)
        except ValueError:
            pass
        else:
            raise AssertionError('Parser accepted an invalid slide sequence')
    plan = {'revision': payload['revision'], 'slide_count': 29, 'word_count': len(source_words), 'chunk_count': len(chunks), 'script_sha256': digest(data), 'reference_sha256': REF_SHA, 'parts_sha256': {p.name: digest(p.read_bytes()) for p in parts}, 'slides': [{'number': s['number'], 'heading': s['heading'], 'word_count': sum(len(p.split()) for p in s['paragraphs'])} for s in slides], 'chunks': chunks}
    (out / 'plan.json').write_text(json.dumps(plan, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({k: v for k, v in plan.items() if k not in ('chunks', 'slides')}, ensure_ascii=False, indent=2), flush=True)
    print('PASS: exact 29-slide coverage, word-for-word chunk equivalence, reference digest, negative parser tests', flush=True)

if __name__ == '__main__':
    main()
