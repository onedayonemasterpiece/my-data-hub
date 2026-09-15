#!/usr/bin/env python3
"""Assemble verified PCM chunks in order and publish a new, traceable MP3."""
from __future__ import annotations
import hashlib
import json
import shutil
import subprocess
import wave
from pathlib import Path

HERE = Path(__file__).resolve().parent

def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()

def command(args: list[str]) -> str:
    result = subprocess.run(args, text=True, capture_output=True, timeout=1200)
    if result.returncode:
        raise RuntimeError(f'{args[0]} failed: {result.stderr[-3000:]}')
    return result.stdout

def main() -> None:
    inputs, all_dir, final = HERE / 'inputs', HERE / 'all-shards', HERE / 'final'
    final.mkdir(parents=True, exist_ok=True)
    plan = json.loads((inputs / 'plan.json').read_text(encoding='utf-8'))
    if sha(inputs / 'narration.json') != plan['script_sha256']:
        raise ValueError('Prepared script changed')
    rows = {}
    receipts = []
    for path in sorted(all_dir.glob('shard-*.json')):
        receipt = json.loads(path.read_text(encoding='utf-8'))
        if receipt['script_sha256'] != plan['script_sha256'] or receipt['reference_sha256'] != plan['reference_sha256']:
            raise ValueError(f'Wrong script/voice in {path.name}')
        if receipt['total_chunks'] != plan['chunk_count']:
            raise ValueError('Wrong expected chunk count')
        expected_selection = [c['index'] for c in plan['chunks'] if (c['index']-1) % receipt['shard_count'] == receipt['shard_index']]
        if receipt['selected_indices'] != expected_selection or [r['index'] for r in receipt['chunks']] != expected_selection:
            raise ValueError(f'Incomplete shard: {path.name}')
        for row in receipt['chunks']:
            index = row['index']
            if index in rows:
                raise ValueError(f'Duplicate chunk {index}')
            rows[index] = row
        receipts.append(receipt)
    expected_indices = list(range(1, plan['chunk_count']+1))
    if sorted(rows) != expected_indices:
        raise ValueError(f'Missing chunks: {sorted(set(expected_indices) - set(rows))}')
    for expected in plan['chunks']:
        row = rows[expected['index']]
        if row['slide'] != expected['slide'] or row['text'] != expected['text']:
            raise ValueError('Spoken input does not match editorial script')
        path = all_dir / 'chunks' / row['wav']
        if sha(path) != row['wav_sha256']:
            raise ValueError(f'WAV digest mismatch: {path.name}')
    wav_path = final / '_master.wav'
    sample_rate, frames_total = 24000, 0
    chapters = []
    warnings = []
    with wave.open(str(wav_path), 'wb') as target:
        target.setnchannels(1)
        target.setsampwidth(2)
        target.setframerate(sample_rate)
        last_slide = None
        for index in expected_indices:
            row = rows[index]
            new_slide = row['slide'] != last_slide
            if index > 1:
                pause_frames = round(sample_rate * (0.9 if new_slide else 0.18))
                target.writeframesraw(b'\0\0' * pause_frames)
                frames_total += pause_frames
            if new_slide:
                if chapters:
                    chapters[-1]['end_seconds'] = frames_total / sample_rate
                chapters.append({'number': len(chapters)+1, 'heading': row['slide'], 'start_seconds': frames_total / sample_rate})
                last_slide = row['slide']
            with wave.open(str(all_dir / 'chunks' / row['wav']), 'rb') as source:
                if (source.getnchannels(), source.getsampwidth(), source.getframerate()) != (1, 2, sample_rate):
                    raise ValueError('Unexpected PCM format')
                seconds = source.getnframes() / sample_rate
                words = len(row['text'].split())
                wpm = words * 60 / seconds
                if not 30 < wpm < 300:
                    raise ValueError(f'Suspicious duration in chunk {index}: {words} words / {seconds:.2f}s')
                if not 65 < wpm < 220:
                    warnings.append({'chunk': index, 'words_per_minute': round(wpm, 1)})
                frames_total += source.getnframes()
                while True:
                    block = source.readframes(65536)
                    if not block:
                        break
                    target.writeframesraw(block)
        chapters[-1]['end_seconds'] = frames_total / sample_rate
    if len(chapters) != 29 or [c['heading'] for c in chapters] != [s['heading'] for s in plan['slides']]:
        raise ValueError('Final audio does not contain exactly the same 29 slides')
    mp3 = final / 'lecture_computer_in_cube_expanded_v2.mp3'
    command(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-y', '-i', str(wav_path), '-map_metadata', '-1', '-c:a', 'libmp3lame', '-b:a', '96k', '-ar', '24000', '-ac', '1', '-metadata', 'title=Компьютер в кубе — расширенный сценарий v2', '-metadata', 'comment=AI-generated narration from the reviewed 29-slide script', str(mp3)])
    command(['ffmpeg', '-v', 'error', '-xerror', '-i', str(mp3), '-f', 'null', '-'])
    probe = json.loads(command(['ffprobe', '-v', 'error', '-show_entries', 'format=duration,size,bit_rate:stream=codec_name,sample_rate,channels', '-of', 'json', str(mp3)]))
    if abs(float(probe['format']['duration']) - frames_total / sample_rate) > 0.2:
        raise ValueError('Encoded duration differs from source PCM')
    manifest = {'revision': plan['revision'], 'script_sha256': plan['script_sha256'], 'reference_sha256': plan['reference_sha256'], 'word_count': plan['word_count'], 'slide_count': len(chapters), 'chunk_count': len(rows), 'model': receipts[0]['model'], 'mp3': mp3.name, 'mp3_sha256': sha(mp3), 'mp3_bytes': mp3.stat().st_size, 'probe': probe, 'pcm_duration_seconds': frames_total / sample_rate, 'duration_warnings': warnings, 'checks': ['exact source text for every chunk', 'no missing or duplicate chunks', 'all WAV checksums', 'same script and reference across all shards', '29 slides in original order', 'full MP3 decode', 'PCM and MP3 duration agreement'], 'not_checked': ['human listening review', 'ASR word-by-word verification']}
    (final / 'final-manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    (final / 'chapters.json').write_text(json.dumps(chapters, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    def stamp(seconds: float) -> str:
        n = int(seconds)
        return f'{n//3600:02d}:{n//60%60:02d}:{n%60:02d}'
    (final / 'chapters.txt').write_text('\n'.join(f"{stamp(c['start_seconds'])}  {c['heading']}" for c in chapters) + '\n', encoding='utf-8')
    for name in ('narration.md', 'narration-spoken-only.txt', 'narration.json', 'plan.json'):
        shutil.copy2(inputs / name, final / name)
    wav_path.unlink()
    print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)

if __name__ == '__main__':
    main()
