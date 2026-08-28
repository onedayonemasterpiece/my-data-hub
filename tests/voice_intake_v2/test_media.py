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


@pytest.mark.asyncio
async def test_real_ffprobe_rejects_aac_lc_with_wrong_channel_and_sample_rate(tmp_path):
    chunk = tmp_path / "wrong-format.m4a"
    process = await asyncio.create_subprocess_exec(
        "ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i",
        "sine=frequency=440:sample_rate=44100:duration=1", "-ac", "2", "-c:a", "aac",
        "-profile:a", "aac_low", "-b:a", "32k", "-y", str(chunk),
    )
    assert await process.wait() == 0
    with pytest.raises(MediaError, match="audio_format_invalid"):
        await BoundedMediaTools().probe(chunk)


@pytest.mark.asyncio
async def test_ffmpeg_timeout_kills_process_and_cleans_private_temporaries(tmp_path, monkeypatch):
    class HangingProcess:
        returncode = None

        def __init__(self):
            self.killed = False

        async def communicate(self):
            await asyncio.sleep(3600)

        def kill(self):
            self.killed = True
            self.returncode = -9

        async def wait(self):
            return self.returncode

    process = HangingProcess()

    async def spawn(*_args, **_kwargs):
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    chunk = tmp_path / "chunk.m4a"
    chunk.write_bytes(b"audio")
    output = tmp_path / "normalized" / "session.mp3"
    with pytest.raises(MediaError, match="ffmpeg_timeout"):
        await BoundedMediaTools(ffmpeg_timeout=0.01).normalize((chunk,), output)
    assert process.killed
    assert not output.exists()
    assert not (output.parent / ".concat.txt").exists()
    assert not (output.parent / ".session.mp3.tmp").exists()


@pytest.mark.asyncio
async def test_normalize_rejects_concat_path_delimiters_and_cleans_list(tmp_path):
    chunk = tmp_path / "bad'name.m4a"
    chunk.write_bytes(b"audio")
    output = tmp_path / "normalized" / "session.mp3"
    with pytest.raises(MediaError, match="audio_path_invalid"):
        await BoundedMediaTools().normalize((chunk,), output)
    assert not (output.parent / ".concat.txt").exists()
