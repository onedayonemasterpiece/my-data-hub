#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import re
import subprocess
import time
from pathlib import Path

MODEL_ID = "Qwen/Qwen3-TTS-12Hz-1.7B-Base"
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


def sentences(text: str) -> list[str]:
    text = re.sub(r"\s+", " ", text.strip())
    if not text:
        return []
    return [p.strip() for p in re.split(r"(?<=[.!?])\s+(?=[«\"А-ЯЁA-Z0-9])", text) if p.strip()]


def chunk_slides(payload: dict, target_words: int = 58, hard_words: int = 72) -> list[dict]:
    chunks: list[dict] = []
    idx = 0
    for slide in payload["slides"]:
        units: list[str] = []
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


def ffprobe(path: Path) -> dict:
    p = subprocess.run([
        "ffprobe", "-v", "error", "-show_entries", "format=duration,size",
        "-of", "json", str(path)
    ], text=True, capture_output=True, check=True, timeout=120)
    return json.loads(p.stdout)


def synthesize(args: argparse.Namespace) -> None:
    import numpy as np
    import soundfile as sf
    import torch
    from qwen_tts import Qwen3TTSModel

    script = Path(args.script).resolve()
    reference = Path(args.reference).resolve()
    out = Path(args.output_dir).resolve()
    chunks_dir = out / "chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)

    payload = json.loads(script.read_text(encoding="utf-8"))
    chunks = chunk_slides(payload)
    selected = [c for c in chunks if (c["index"] - 1) % args.shard_count == args.shard_index]
    print(
        f"total_chunks={len(chunks)} selected={len(selected)} shard={args.shard_index}/{args.shard_count} "
        f"words={sum(len(c['text'].split()) for c in selected)}",
        flush=True,
    )
    if not selected:
        raise RuntimeError("shard has no chunks")

    torch.set_num_threads(max(1, min(4, os.cpu_count() or 4)))
    try:
        torch.set_num_interop_threads(1)
    except RuntimeError:
        pass

    dtype_name = os.getenv("TTS_DTYPE", "float32").strip().lower()
    dtype = torch.bfloat16 if dtype_name == "bfloat16" else torch.float32
    print(f"loading model={MODEL_ID} dtype={dtype_name}", flush=True)
    load_started = time.time()
    model = Qwen3TTSModel.from_pretrained(
        MODEL_ID,
        device_map="cpu",
        dtype=dtype,
        attn_implementation="eager",
        low_cpu_mem_usage=True,
    )
    prompt = model.create_voice_clone_prompt(
        ref_audio=str(reference),
        ref_text=REFERENCE_TEXT,
        x_vector_only_mode=False,
    )
    print(f"model_ready_seconds={time.time() - load_started:.2f}", flush=True)

    manifest = {
        "model": MODEL_ID,
        "dtype": dtype_name,
        "script_sha256": sha256_file(script),
        "reference_sha256": sha256_file(reference),
        "shard_index": args.shard_index,
        "shard_count": args.shard_count,
        "total_chunks": len(chunks),
        "selected_indices": [c["index"] for c in selected],
        "started_at": time.time(),
        "chunks": [],
    }

    for pos, chunk in enumerate(selected, start=1):
        n = chunk["index"]
        seed = 20260915 + n
        wav_path = chunks_dir / f"{n:03d}.wav"
        t0 = time.time()
        print(f"chunk {pos}/{len(selected)} global={n:03d} slide={chunk['slide']} words={len(chunk['text'].split())}", flush=True)
        torch.manual_seed(seed)
        np.random.seed(seed)
        with torch.inference_mode():
            wavs, sr = model.generate_voice_clone(
                text=chunk["text"],
                language="Russian",
                voice_clone_prompt=prompt,
                **GENERATION_KWARGS,
            )
        if not wavs or len(wavs[0]) < 1000:
            raise RuntimeError(f"chunk {n}: empty waveform")
        sf.write(str(wav_path), wavs[0], sr, subtype="PCM_16")
        row = {
            **chunk,
            "seed": seed,
            "sample_rate": int(sr),
            "wav": wav_path.name,
            "wav_sha256": sha256_file(wav_path),
            "probe": ffprobe(wav_path),
            "generation_seconds": time.time() - t0,
        }
        manifest["chunks"].append(row)
        (out / "manifest.partial.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        del wavs
        gc.collect()

    manifest["finished_at"] = time.time()
    (out / f"shard-{args.shard_index}.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"shard complete: {len(selected)} chunks", flush=True)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--script", required=True)
    p.add_argument("--reference", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--shard-index", type=int, required=True)
    p.add_argument("--shard-count", type=int, required=True)
    return p


if __name__ == "__main__":
    a = parser().parse_args()
    if not (0 <= a.shard_index < a.shard_count):
        raise SystemExit("invalid shard index/count")
    synthesize(a)
