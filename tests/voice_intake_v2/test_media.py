from __future__ import annotations

import asyncio

import pytest

from my_data_hub.voice_intake_v2.media import BoundedMediaTools, MediaError


@pytest.mark.asyncio
async def test_real_ffprobe_accepts_aac_lc_mono_16khz_and_ffmpeg_normalizes(tmp_path):
    chunk = tmp_path / "chunk.m4a"
    process = await asyncio.create_subprocess_exec(
        "ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i",
        "sine=frequency=440:sample_rate=16000:duration=1", "-ac", "1", "-c:a", "aac",
        "-profile:a", "aac_low", "-b:a", "32k", "-y", str(chunk),
    )
    assert await process.wait() == 0
    tools = BoundedMediaTools(ffprobe_timeout=10, ffmpeg_timeout=30)
    probe = await tools.probe(chunk)
    assert probe.codec == "aac" and probe.profile == "LC"
    assert probe.sample_rate_hz == 16000 and probe.channels == 1
    output = tmp_path / "normalized" / "session.mp3"
    await tools.normalize((chunk,), output)
    assert output.is_file() and output.stat().st_size > 0


@pytest.mark.asyncio
async def test_real_ffprobe_rejects_invalid_audio(tmp_path):
    path = tmp_path / "invalid.m4a"
    path.write_bytes(b"not audio")
    with pytest.raises(MediaError, match="audio_invalid"):
        await BoundedMediaTools().probe(path)
