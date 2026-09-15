#!/usr/bin/env python3
"""Insert audible slide numbers/titles; never synthesize or change the lecture body."""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
import subprocess
import wave
import zipfile
from pathlib import Path

SCRIPT_SHA = '21287df14140bfb7843ca05a42985a0bc9cfdf2c8876a15f7a7c37122fbcae14'
AUDIO_SHA = 'ea20d65ac476ac2409d7cdcfe024e3f6d58233dbe974bf36e61e5a268ac9c055'
REF_SHA = 'bd92c7a4343e2e07256942bb036d7db9ef885eda5985109f140849ce081ccf5a'
BASE_RUN = 34979607472
RATE = 24000
GAP = 0.65
ORDINALS = ('первый|второй|третий|четвёртый|пятый|шестой|седьмой|восьмой|девятый|'
            'десятый|одиннадцатый|двенадцатый|тринадцатый|четырнадцатый|пятнадцатый|'
            'шестнадцатый|семнадцатый|восемнадцатый|девятнадцатый|двадцатый|'
            'двадцать первый|двадцать второй|двадцать третий|двадцать четвёртый|'
            'двадцать пятый|двадцать шестой|двадцать седьмой|двадцать восьмой|двадцать девятый').split('|')


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def read_json(path: Path) -> dict | list:
    return json.loads(path.read_text(encoding='utf-8'))


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def run(args: list[str]) -> str:
    p = subprocess.run(args, text=True, capture_output=True, timeout=900)
    if p.returncode:
        raise RuntimeError(f'{args[0]} failed: {p.stderr[-3000:]}')
    return p.stdout


def prepare(root: Path) -> dict:
    inputs = root / 'inputs'
    if sha(inputs / 'narration.json') != SCRIPT_SHA or sha(inputs / 'reference.ogg') != REF_SHA:
        raise ValueError('Wrong reviewed lecture or voice reference')
    original = read_json(inputs / 'narration.json')
    if [s['number'] for s in original['slides']] != list(range(1, 30)):
        raise ValueError('Exactly 29 ordered slides are required')
    slides = []
    for s, ordinal in zip(original['slides'], ORDINALS):
        prefix = f"Слайд {s['number']}. "
        if not s['heading'].startswith(prefix):
            raise ValueError('Incorrect slide number in heading')
        title = s['heading'][len(prefix):].strip().rstrip('.?!')
        text = f'Слайд {ordinal}. {title}.'
        slides.append({'number': s['number'], 'heading': s['heading'], 'paragraphs': [text]})
    payload = {'title': 'Голосовые объявления слайдов', 'base_script_sha256': SCRIPT_SHA, 'slides': slides}
    write_json(inputs / 'cues.json', payload)
    print('Prepared exactly 29 short announcements; lecture body is not passed to TTS', flush=True)
    return payload


def synthesize(root: Path, worker_path: Path, shard: int, count: int) -> None:
    payload = prepare(root)
    spec = importlib.util.spec_from_file_location('proven_tts_worker', worker_path)
    if spec is None or spec.loader is None:
        raise RuntimeError('Unable to import proven TTS worker')
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    chunks = worker.chunk_slides(payload)
    if len(chunks) != 29 or [c['slide'] for c in chunks] != [s['heading'] for s in payload['slides']]:
        raise ValueError('Every slide must have exactly one announcement')
    worker.GENERATION_KWARGS['max_new_tokens'] = 256
    worker.synthesize(argparse.Namespace(script=str(root / 'inputs/cues.json'),
        reference=str(root / 'inputs/reference.ogg'), output_dir=str(root / f'part-{shard}'),
        shard_index=shard, shard_count=count))


def pcm(path: Path) -> bytes:
    with wave.open(str(path), 'rb') as src:
        if (src.getnchannels(), src.getsampwidth(), src.getframerate()) != (1, 2, RATE):
            raise ValueError(f'Unexpected WAV format: {path}')
        return src.readframes(src.getnframes())


def save_pcm(path: Path, data: bytes) -> None:
    with wave.open(str(path), 'wb') as dst:
        dst.setnchannels(1)
        dst.setsampwidth(2)
        dst.setframerate(RATE)
        dst.writeframes(data)


def encode(wav: Path, mp3: Path, title: str) -> None:
    run(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-y', '-i', str(wav),
         '-map_metadata', '-1', '-c:a', 'libmp3lame', '-b:a', '96k', '-ar', str(RATE), '-ac', '1',
         '-metadata', f'title={title}', '-metadata', 'album=Компьютер в кубе — по слайдам', str(mp3)])


def stamp(seconds: float) -> str:
    value = int(seconds)
    return f'{value//3600:02d}:{value//60%60:02d}:{value%60:02d}'


def assemble(root: Path) -> None:
    payload = prepare(root)
    base, parts, final = root / 'base', root / 'announcements', root / 'final'
    final.mkdir(parents=True, exist_ok=True)
    metadata = read_json(base / 'final-manifest.json')
    original_mp3 = base / 'lecture_computer_in_cube_expanded_v2.mp3'
    if sha(original_mp3) != AUDIO_SHA or metadata['script_sha256'] != SCRIPT_SHA:
        raise ValueError('Base audio is not the delivered reviewed v2')
    chapters = read_json(base / 'chapters.json')
    if [c['heading'] for c in chapters] != [s['heading'] for s in payload['slides']]:
        raise ValueError('Chapter order disagrees with source headings')
    cues_sha, cues = sha(root / 'inputs/cues.json'), {}
    for path in sorted(parts.glob('shard-*.json')):
        receipt = read_json(path)
        if receipt['script_sha256'] != cues_sha or receipt['reference_sha256'] != REF_SHA:
            raise ValueError('Wrong header text or voice')
        for row in receipt['chunks']:
            n = row['index']
            if n in cues or n not in range(1, 30):
                raise ValueError('Duplicate or unexpected announcement')
            expected = payload['slides'][n-1]
            if row['text'] != expected['paragraphs'][0] or row['slide'] != expected['heading']:
                raise ValueError('Header text or number mismatch')
            path = parts / 'chunks' / row['wav']
            if sha(path) != row['wav_sha256']:
                raise ValueError('Header WAV checksum mismatch')
            data = pcm(path)
            duration = len(data) / (2 * RATE)
            if not 0.8 < duration < 20.5:
                raise ValueError(f'Suspicious announcement duration on slide {n}: {duration}')
            cues[n] = (data, row)
    if sorted(cues) != list(range(1, 30)):
        raise ValueError('Not all 29 announcements were synthesized')
    original_wav = root / 'original.wav'
    run(['ffmpeg', '-v', 'error', '-y', '-i', str(original_mp3), '-ac', '1', '-ar', str(RATE),
         '-c:a', 'pcm_s16le', str(original_wav)])
    original = pcm(original_wav)
    if len(original) // 2 != round(metadata['pcm_duration_seconds'] * RATE):
        raise ValueError('Decoded audio does not match source chapter timing')
    frames, previous_end, body_bytes = 0, 0, 0
    body_hash, full_hash = hashlib.sha256(), hashlib.sha256(original).hexdigest()
    output_chapters, cue_rows = [], []
    master, one_slide = root / 'master.wav', root / 'one-slide.wav'
    split_dir = root / 'per-slide'
    split_dir.mkdir(exist_ok=True)
    with wave.open(str(master), 'wb') as dst:
        dst.setnchannels(1); dst.setsampwidth(2); dst.setframerate(RATE)
        for chapter in chapters:
            n = chapter['number']
            start, end = round(chapter['start_seconds']*RATE), round(chapter['end_seconds']*RATE)
            if start != previous_end or end <= start:
                raise ValueError('Gap, overlap or reversed base chapter')
            body = original[start*2:end*2]
            previous_end = end
            body_hash.update(body)
            body_bytes += len(body)
            announcement, receipt = cues[n]
            gap = b'\0\0' * round(GAP * RATE)
            section = announcement + gap + body
            row = {'number': n, 'heading': chapter['heading'], 'announcement': receipt['text'],
                   'start_seconds': frames/RATE,
                   'body_start_seconds': (frames + len(announcement)//2 + len(gap)//2)/RATE,
                   'body_duration_seconds': len(body)/(2*RATE),
                   'source_start_seconds': chapter['start_seconds'],
                   'source_end_seconds': chapter['end_seconds'],
                   'body_pcm_sha256': hashlib.sha256(body).hexdigest()}
            dst.writeframesraw(section)
            frames += len(section)//2
            row['end_seconds'] = frames/RATE
            output_chapters.append(row)
            save_pcm(one_slide, section)
            encode(one_slide, split_dir / f'slide-{n:02d}.mp3', chapter['heading'])
            cue_rows.append(receipt)
    if body_bytes != len(original) or body_hash.hexdigest() != full_hash:
        raise ValueError('Original narration was changed or omitted')
    output = final / 'quantum_lecture_v2_with_slide_announcements.mp3'
    encode(master, output, 'Компьютер в кубе — с голосовыми номерами слайдов')
    run(['ffmpeg', '-v', 'error', '-xerror', '-i', str(output), '-f', 'null', '-'])
    probe = json.loads(run(['ffprobe', '-v', 'error', '-show_entries',
        'format=duration,size,bit_rate:stream=codec_name,sample_rate,channels', '-of', 'json', str(output)]))
    if abs(float(probe['format']['duration']) - frames/RATE) > 0.2:
        raise ValueError('Encoded duration mismatch')
    write_json(final / 'chapters.json', output_chapters)
    (final / 'chapters.txt').write_text('\n'.join(f"{stamp(c['start_seconds'])}  {c['heading']}" for c in output_chapters) + '\n', encoding='utf-8')
    (final / 'announcements.txt').write_text('\n'.join(r['text'] for r in cue_rows) + '\n', encoding='utf-8')
    write_json(final / 'announcements-receipt.json', cue_rows)
    with zipfile.ZipFile(final / 'quantum_lecture_29_slides_mp3.zip', 'w', zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(split_dir.glob('*.mp3')):
            archive.write(path, path.name)
        archive.write(final / 'chapters.txt', 'chapters.txt')
    write_json(final / 'final-manifest.json', {'base_run_id': BASE_RUN, 'base_script_sha256': SCRIPT_SHA,
        'base_mp3_sha256': AUDIO_SHA, 'reference_sha256': REF_SHA, 'mp3': output.name,
        'mp3_sha256': sha(output), 'probe': probe, 'slide_count': 29, 'announcement_count': 29,
        'body_resynthesized': False, 'body_pcm_before_encoding_sha256': full_hash,
        'body_pcm_coverage_bytes': body_bytes, 'pcm_duration_seconds': frames/RATE,
        'checks': ['29 spoken numbers and titles in original order', 'all original decoded PCM retained byte-for-byte before final encoding',
                   'all announcement texts and WAV checksums', 'exact source timeline coverage', 'full MP3 decode', '29 standalone MP3 files'],
        'not_checked': ['human listening review of all announcements']})
    print(json.dumps(read_json(final / 'final-manifest.json'), ensure_ascii=False, indent=2), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['prepare', 'synthesize', 'assemble'])
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--worker', type=Path)
    parser.add_argument('--shard', type=int, default=0)
    parser.add_argument('--count', type=int, default=8)
    args = parser.parse_args()
    if args.mode == 'prepare':
        prepare(args.root)
    elif args.mode == 'synthesize':
        if args.worker is None or not 0 <= args.shard < args.count:
            parser.error('Provide a worker and a valid shard/count')
        synthesize(args.root, args.worker, args.shard, args.count)
    else:
        assemble(args.root)
