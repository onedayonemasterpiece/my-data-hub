#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

MODEL_ID = "Qwen/Qwen3-TTS-12Hz-1.7B-Base"
DATASET = "zigomaro/audiofiles-for-tts"
SOURCE_NAME = "audio_2026-09-02_15-37-11-kazakova.ogg"
EXPECTED_SOURCE_SHA256 = "4a4d1732ae7a5b6ed7564639196665b512381b76ef72417896d8638caad27228"
REFERENCE_START = 0.0
REFERENCE_END = 10.30
REFERENCE_TEXT = (
    "Привет! Твоя тема с помощником на основе нейросетей может быть актуальна. "
    "Десять-одиннадцать будут круглые столы, шестнадцатого будет ещё одно мероприятие."
)
GENERATION_KWARGS = dict(
    do_sample=True,
    temperature=0.9,
    top_p=1.0,
    top_k=50,
    repetition_penalty=1.05,
    subtalker_dosample=True,
    subtalker_temperature=0.9,
    subtalker_top_p=1.0,
    subtalker_top_k=50,
    max_new_tokens=1024,
)

os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
os.environ.setdefault("OMP_NUM_THREADS", "4")
os.environ.setdefault("MKL_NUM_THREADS", "4")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "4")
os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def run(cmd: list[str], *, timeout: int = 1800, check: bool = True) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(map(str, cmd)), flush=True)
    p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
    if p.stdout:
        print(p.stdout[-8000:], flush=True)
    if check and p.returncode != 0:
        raise RuntimeError(f"command failed rc={p.returncode}: {' '.join(cmd)}")
    return p


def find_reference_source(work: Path) -> Path:
    candidates = []
    for root in (Path("/kaggle/input"), Path.cwd(), work):
        if root.exists():
            candidates.extend(root.rglob(SOURCE_NAME))
    for p in candidates:
        try:
            if sha256_file(p) == EXPECTED_SOURCE_SHA256:
                return p
        except Exception:
            pass

    dl = work / "reference_dataset"
    dl.mkdir(parents=True, exist_ok=True)
    zip_path = dl / "audiofiles-for-tts.zip"
    url = "https://www.kaggle.com/api/v1/datasets/download/zigomaro/audiofiles-for-tts"
    try:
        print(f"Downloading public Kaggle dataset: {url}", flush=True)
        urllib.request.urlretrieve(url, zip_path)
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(dl)
    except Exception as e:
        print(f"Direct Kaggle download failed: {e!r}", flush=True)

    matches = list(dl.rglob(SOURCE_NAME))
    if len(matches) == 1 and sha256_file(matches[0]) == EXPECTED_SOURCE_SHA256:
        return matches[0]

    kaggle = shutil.which("kaggle")
    if kaggle:
        run([kaggle, "datasets", "download", "-d", DATASET, "-p", str(dl), "--unzip"], timeout=600, check=False)
        matches = list(dl.rglob(SOURCE_NAME))
        for p in matches:
            if sha256_file(p) == EXPECTED_SOURCE_SHA256:
                return p

    raise RuntimeError(
        f"Reference audio {SOURCE_NAME} with expected SHA256 was not found. "
        f"On Kaggle, add dataset {DATASET} as notebook input, or provide Kaggle credentials."
    )


def export_reference(source: Path, out: Path) -> None:
    run([
        "ffmpeg", "-hide_banner", "-nostats", "-y",
        "-ss", f"{REFERENCE_START:.3f}", "-to", f"{REFERENCE_END:.3f}",
        "-i", str(source), "-ar", "24000", "-ac", "1", "-c:a", "pcm_s16le", str(out),
    ], timeout=180)
    if not out.exists() or out.stat().st_size < 100_000:
        raise RuntimeError("reference clip export failed")


def sentences(text: str) -> list[str]:
    text = re.sub(r"\s+", " ", text.strip())
    if not text:
        return []
    parts = re.split(r"(?<=[.!?])\s+(?=[«\"А-ЯЁA-Z0-9])", text)
    return [p.strip() for p in parts if p.strip()]


def chunk_slides(payload: dict, target_words: int = 58, hard_words: int = 72) -> list[dict]:
    chunks: list[dict] = []
    idx = 0
    for slide in payload["slides"]:
        units = []
        for paragraph in slide["paragraphs"]:
            units.extend(sentences(paragraph))
        buf: list[str] = []
        wc = 0
        for unit in units:
            uw = len(unit.split())
            if buf and (wc + uw > hard_words or (wc >= target_words and unit.endswith((".", "!", "?")))):
                idx += 1
                chunks.append({"index": idx, "slide": slide["heading"], "text": " ".join(buf)})
                buf, wc = [], 0
            if uw > hard_words:
                pieces = [x.strip() for x in re.split(r"(?<=[;:])\s+", unit) if x.strip()]
                for piece in pieces:
                    pw = len(piece.split())
                    if buf and wc + pw > hard_words:
                        idx += 1
                        chunks.append({"index": idx, "slide": slide["heading"], "text": " ".join(buf)})
                        buf, wc = [], 0
                    buf.append(piece)
                    wc += pw
            else:
                buf.append(unit)
                wc += uw
        if buf:
            idx += 1
            chunks.append({"index": idx, "slide": slide["heading"], "text": " ".join(buf)})
    return chunks


def synthesize_one(args) -> None:
    import numpy as np
    import soundfile as sf
    import torch
    from qwen_tts import Qwen3TTSModel

    torch.set_num_threads(max(1, min(4, os.cpu_count() or 4)))
    try:
        torch.set_num_interop_threads(1)
    except RuntimeError:
        pass

    dtype_name = os.getenv("TTS_DTYPE", "float32").strip().lower()
    dtype = torch.bfloat16 if dtype_name == "bfloat16" else torch.float32

    model = Qwen3TTSModel.from_pretrained(
        MODEL_ID,
        device_map="cpu",
        dtype=dtype,
        attn_implementation="eager",
        low_cpu_mem_usage=True,
    )
    prompt = model.create_voice_clone_prompt(
        ref_audio=args.reference,
        ref_text=REFERENCE_TEXT,
        x_vector_only_mode=False,
    )
    seed = int(args.seed)
    torch.manual_seed(seed)
    np.random.seed(seed)
    wavs, sr = model.generate_voice_clone(
        text=args.text,
        language="Russian",
        voice_clone_prompt=prompt,
        **GENERATION_KWARGS,
    )
    if not wavs or len(wavs[0]) < 1000:
        raise RuntimeError("empty waveform")
    sf.write(args.output, wavs[0], sr, subtype="PCM_16")
    del wavs, prompt, model
    gc.collect()


def main_run(args) -> None:
    root = Path(args.workdir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    chunks_dir = root / "chunks"
    chunks_dir.mkdir(exist_ok=True)

    payload = json.loads(Path(args.script).read_text(encoding="utf-8"))
    chunks = chunk_slides(payload)
    (root / "chunks.json").write_text(json.dumps(chunks, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Prepared {len(chunks)} chunks, {sum(len(c['text'].split()) for c in chunks)} words", flush=True)

    source = find_reference_source(root)
    print("Reference source:", source, sha256_file(source), flush=True)
    reference = root / "reference.wav"
    export_reference(source, reference)

    helper = Path(__file__).resolve()
    manifest = {
        "model": MODEL_ID,
        "dataset": DATASET,
        "source_name": SOURCE_NAME,
        "source_sha256": EXPECTED_SOURCE_SHA256,
        "script_sha256": sha256_file(Path(args.script)),
        "started_at": time.time(),
        "chunks": [],
    }
    for chunk in chunks:
        n = chunk["index"]
        wav = chunks_dir / f"{n:03d}.wav"
        seed = 20260915 + n
        started = time.time()
        run([
            sys.executable, str(helper), "one",
            "--reference", str(reference),
            "--output", str(wav),
            "--seed", str(seed),
            "--text", chunk["text"],
        ], timeout=3600)
        probe = run([
            "ffprobe", "-v", "error", "-show_entries", "format=duration,size",
            "-of", "json", str(wav)
        ], timeout=120).stdout
        manifest["chunks"].append({
            **chunk,
            "seed": seed,
            "wav": wav.name,
            "wav_sha256": sha256_file(wav),
            "probe": json.loads(probe or "{}"),
            "generation_seconds": time.time() - started,
        })
        (root / "manifest.partial.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    concat = root / "concat.txt"
    concat.write_text("".join(f"file '{(chunks_dir / f'{c['index']:03d}.wav').as_posix()}'\n" for c in chunks), encoding="utf-8")
    mp3 = root / args.output
    run([
        "ffmpeg", "-hide_banner", "-nostats", "-y",
        "-f", "concat", "-safe", "0", "-i", str(concat),
        "-c:a", "libmp3lame", "-b:a", "48k", str(mp3)
    ], timeout=1200)
    decode = run(["ffmpeg", "-v", "error", "-i", str(mp3), "-f", "null", "-"], timeout=300, check=False)
    if decode.returncode != 0:
        raise RuntimeError("final MP3 is not decodable")

    final_probe = json.loads(run([
        "ffprobe", "-v", "error", "-show_entries",
        "format=duration,size,bit_rate,format_name:stream=codec_name,sample_rate,channels",
        "-of", "json", str(mp3)
    ], timeout=120).stdout)

    manifest["finished_at"] = time.time()
    manifest["final"] = {
        "path": mp3.name,
        "sha256": sha256_file(mp3),
        "bytes": mp3.stat().st_size,
        "probe": final_probe,
    }
    (root / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest["final"], ensure_ascii=False, indent=2), flush=True)


def build_parser():
    p = argparse.ArgumentParser()
    sp = p.add_subparsers(dest="cmd", required=True)

    p_run = sp.add_parser("run")
    p_run.add_argument("--script", required=True)
    p_run.add_argument("--workdir", default="tts_out")
    p_run.add_argument("--output", default="lecture_computer_in_cube.mp3")

    p_one = sp.add_parser("one")
    p_one.add_argument("--reference", required=True)
    p_one.add_argument("--output", required=True)
    p_one.add_argument("--seed", required=True)
    p_one.add_argument("--text", required=True)
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    if args.cmd == "one":
        synthesize_one(args)
    else:
        main_run(args)
