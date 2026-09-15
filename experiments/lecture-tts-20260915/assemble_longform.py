#!/usr/bin/env python3
from __future__ import annotations
import argparse, hashlib, json, subprocess, time
from pathlib import Path


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024*1024), b''):
            h.update(block)
    return h.hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--input-dir', required=True)
    ap.add_argument('--output', required=True)
    ap.add_argument('--expected-chunks', type=int, default=53)
    args = ap.parse_args()
    root = Path(args.input_dir).resolve()
    out = Path(args.output).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    wavs = sorted(root.rglob('*.wav'))
    by_num = {}
    for p in wavs:
        try:
            n = int(p.stem)
        except ValueError:
            continue
        if n in by_num:
            raise RuntimeError(f'duplicate chunk {n}: {by_num[n]} and {p}')
        by_num[n] = p
    expected = list(range(1, args.expected_chunks + 1))
    missing = [n for n in expected if n not in by_num]
    extra = sorted(n for n in by_num if n not in expected)
    print(f'found={len(by_num)} missing={missing} extra={extra}', flush=True)
    if missing or extra:
        raise RuntimeError(f'chunk coverage invalid: missing={missing} extra={extra}')
    concat = out.parent / 'concat.txt'
    concat.write_text(''.join(f"file '{by_num[n].as_posix()}'\n" for n in expected), encoding='utf-8')
    subprocess.run([
        'ffmpeg','-hide_banner','-nostats','-y','-f','concat','-safe','0','-i',str(concat),
        '-c:a','libmp3lame','-b:a','64k',str(out)
    ], check=True, timeout=1800)
    subprocess.run(['ffmpeg','-v','error','-i',str(out),'-f','null','-'], check=True, timeout=300)
    probe = subprocess.run([
        'ffprobe','-v','error','-show_entries',
        'format=duration,size,bit_rate,format_name:stream=codec_name,sample_rate,channels',
        '-of','json',str(out)
    ], check=True, text=True, capture_output=True, timeout=120)
    manifest = {
        'created_at': time.time(),
        'chunk_count': len(expected),
        'output': out.name,
        'bytes': out.stat().st_size,
        'sha256': sha256_file(out),
        'probe': json.loads(probe.stdout),
        'chunk_sha256': {str(n): sha256_file(by_num[n]) for n in expected},
    }
    (out.parent / 'final-manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)

if __name__ == '__main__':
    main()
