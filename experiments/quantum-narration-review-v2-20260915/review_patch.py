#!/usr/bin/env python3
"""One explicit editorial correction; reuse audio only after exact text/hash checks."""
from __future__ import annotations
import argparse
import hashlib
import json
import runpy
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / 'quantum-narration-v2-20260915'
WORKER = ROOT.parent / 'lecture-tts-20260915' / 'longform_shard.py'
BASE_RUN = 34977014409
BASE_SHA = 'aaf1800120a0ad89e20f515f2cee1df0652d593a178c7911516818891f9ad8e4'
OLD = 'Названия напоминают о людях, которые развивали математику и вычислительную технику.'
NEW = 'Некоторые из этих систем названы в честь выдающихся российских учёных.'
PATCH_INDEX = 18

def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()

def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

def prepare() -> None:
    inputs = ROOT / 'inputs'
    if sha(inputs / 'narration.json') != BASE_SHA:
        raise ValueError('Unexpected base narration')
    payload = json.loads((inputs / 'narration.json').read_text(encoding='utf-8'))
    plan = json.loads((inputs / 'plan.json').read_text(encoding='utf-8'))
    original_chunks = plan['chunks']
    replacements = 0
    for slide in payload['slides']:
        for i, paragraph in enumerate(slide['paragraphs']):
            if OLD in paragraph:
                if slide['number'] != 7 or paragraph.count(OLD) != 1:
                    raise ValueError('Unexpected replacement target')
                slide['paragraphs'][i] = paragraph.replace(OLD, NEW)
                replacements += 1
    if replacements != 1 or len(OLD.split()) != len(NEW.split()):
        raise ValueError('Expected exactly one equal-length editorial correction')
    chunks = runpy.run_path(str(WORKER))['chunk_slides'](payload)
    changed = [a['index'] for a, b in zip(chunks, original_chunks) if a != b]
    if len(chunks) != len(original_chunks) or changed != [PATCH_INDEX]:
        raise ValueError(f'Unexpected changed chunks: {changed}')
    write_json(inputs / 'narration.json', payload)
    for name in ('narration.md', 'narration-spoken-only.txt'):
        text = (inputs / name).read_text(encoding='utf-8')
        if text.count(OLD) != 1:
            raise ValueError('Text export does not match the JSON')
        (inputs / name).write_text(text.replace(OLD, NEW), encoding='utf-8')
    plan['script_sha256'] = sha(inputs / 'narration.json')
    plan['chunks'] = chunks
    plan['editorial_patch'] = {'base_run_id': BASE_RUN, 'base_script_sha256': BASE_SHA, 'changed_chunks': changed, 'slide': 7, 'old': OLD, 'new': NEW, 'reason': 'Not every computer on slide 7 is named after a scientist; avoid that overgeneralization.'}
    write_json(inputs / 'plan.json', plan)
    print(json.dumps({'script_sha256': plan['script_sha256'], 'changed_chunks': changed, 'chunk_count': len(chunks), 'word_count': plan['word_count']}, ensure_ascii=False), flush=True)

def finalize() -> None:
    inputs = ROOT / 'inputs'
    plan = json.loads((inputs / 'plan.json').read_text(encoding='utf-8'))
    if sha(inputs / 'narration.json') != plan['script_sha256']:
        raise ValueError('Final script checksum mismatch')
    rows = {}
    origins = {}
    for path in sorted((ROOT / 'base-shards').glob('shard-*.json')):
        m = json.loads(path.read_text(encoding='utf-8'))
        if m['script_sha256'] != BASE_SHA or m['reference_sha256'] != plan['reference_sha256']:
            raise ValueError('Wrong base script or reference')
        selected = [c['index'] for c in plan['chunks'] if (c['index']-1) % m['shard_count'] == m['shard_index']]
        if m['selected_indices'] != selected or [c['index'] for c in m['chunks']] != selected:
            raise ValueError('Incomplete source shard')
        for row in m['chunks']:
            i = row['index']
            if i in rows:
                raise ValueError('Duplicate base chunk')
            rows[i] = row
            origins[i] = (ROOT / 'base-shards' / 'chunks' / row['wav'], BASE_SHA)
    if sorted(rows) != list(range(1, plan['chunk_count']+1)):
        raise ValueError('Missing original audio chunks')
    patches = list((ROOT / 'reviewed-chunk').glob('shard-*.json'))
    if len(patches) != 1:
        raise ValueError('Expected one replacement receipt')
    patch = json.loads(patches[0].read_text(encoding='utf-8'))
    if patch['script_sha256'] != plan['script_sha256'] or patch['reference_sha256'] != plan['reference_sha256'] or patch['selected_indices'] != [PATCH_INDEX] or [c['index'] for c in patch['chunks']] != [PATCH_INDEX]:
        raise ValueError('Invalid replacement audio provenance')
    rows[PATCH_INDEX] = patch['chunks'][0]
    origins[PATCH_INDEX] = (ROOT / 'reviewed-chunk' / 'chunks' / rows[PATCH_INDEX]['wav'], plan['script_sha256'])
    staging = ROOT / 'all-shards'
    (staging / 'chunks').mkdir(parents=True, exist_ok=True)
    for expected in plan['chunks']:
        row = rows[expected['index']]
        path, source_sha = origins[expected['index']]
        if row['text'] != expected['text'] or row['slide'] != expected['slide'] or sha(path) != row['wav_sha256']:
            raise ValueError(f'Content mismatch at chunk {expected["index"]}')
        row['source_script_sha256'] = source_sha
        row['source_run_id'] = BASE_RUN if source_sha == BASE_SHA else 'review-patch-current-run'
        shutil.copy2(path, staging / 'chunks' / row['wav'])
    # This is explicitly an assembly receipt, not a claim that all chunks were re-synthesized.
    receipt = {'receipt_type': 'text-verified-editorial-assembly', 'model': patch['model'], 'script_sha256': plan['script_sha256'], 'reference_sha256': plan['reference_sha256'], 'shard_index': 0, 'shard_count': 1, 'total_chunks': plan['chunk_count'], 'selected_indices': list(range(1, plan['chunk_count']+1)), 'chunks': [rows[i] for i in sorted(rows)], 'editorial_patch': plan['editorial_patch']}
    write_json(staging / 'shard-0.json', receipt)
    subprocess.run([sys.executable, str(ROOT / 'assemble.py')], check=True, timeout=1200)
    manifest_path = ROOT / 'final' / 'final-manifest.json'
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    manifest['editorial_patch'] = plan['editorial_patch']
    manifest['reused_unchanged_chunks'] = plan['chunk_count'] - 1
    manifest['newly_synthesized_chunks'] = [PATCH_INDEX]
    write_json(manifest_path, manifest)
    write_json(ROOT / 'final' / 'chunk-source-receipt.json', receipt)
    print('PASS: corrected slide 7; every retained audio chunk matches the final text exactly', flush=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['prepare', 'finalize'])
    args = parser.parse_args()
    prepare() if args.mode == 'prepare' else finalize()
