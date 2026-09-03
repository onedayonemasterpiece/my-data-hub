from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path


class MediaError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class MediaProbe:
    duration_ms: int
    codec: str
    profile: str
    sample_rate_hz: int
    channels: int


class BoundedMediaTools:
    def __init__(self, *, ffprobe_timeout: int = 15, ffmpeg_timeout: int = 600) -> None:
        self.ffprobe_timeout = ffprobe_timeout
        self.ffmpeg_timeout = ffmpeg_timeout

    async def probe(self, path: Path) -> MediaProbe:
        command = (
            "ffprobe", "-v", "error", "-show_entries",
            "format=format_name,duration:stream=codec_name,profile,sample_rate,channels",
            "-select_streams", "a:0", "-of", "json", str(path),
        )
        try:
            process = await asyncio.create_subprocess_exec(
                *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _stderr = await asyncio.wait_for(process.communicate(), timeout=self.ffprobe_timeout)
        except TimeoutError as exc:
            if "process" in locals():
                process.kill()
                await process.wait()
            raise MediaError("ffprobe_timeout") from exc
        if process.returncode != 0 or len(stdout) > 64 * 1024:
            raise MediaError("audio_invalid")
        try:
            value = json.loads(stdout)
            stream = value["streams"][0]
            names = set(value["format"]["format_name"].split(","))
            probe = MediaProbe(
                duration_ms=round(float(value["format"]["duration"]) * 1000),
                codec=str(stream["codec_name"]), profile=str(stream.get("profile", "")),
                sample_rate_hz=int(stream["sample_rate"]), channels=int(stream["channels"]),
            )
        except (KeyError, IndexError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise MediaError("audio_probe_invalid") from exc
        if not names.intersection({"mov", "mp4", "m4a", "3gp", "3g2", "mj2"}):
            raise MediaError("audio_container_invalid")
        if probe.codec != "aac" or probe.profile not in {"LC", "AAC LC", "Low Complexity"}:
            raise MediaError("audio_codec_invalid")
        if probe.channels != 1 or probe.sample_rate_hz != 16_000:
            raise MediaError("audio_format_invalid")
        if probe.duration_ms <= 0:
            raise MediaError("audio_duration_invalid")
        return probe

    async def normalize(self, chunks: tuple[Path, ...], output: Path) -> None:
        if not chunks:
            raise MediaError("chunks_missing")
        output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        list_path = output.parent / ".concat.txt"
        temp_path = output.parent / f".{output.name}.tmp"
        try:
            lines: list[str] = []
            for path in chunks:
                resolved = path.resolve()
                # The store controls these paths. Reject delimiter characters
                # rather than attempting shell-like escaping for concat files.
                if "'" in str(resolved) or "\n" in str(resolved) or "\r" in str(resolved):
                    raise MediaError("audio_path_invalid")
                lines.append(f"file '{resolved}'\n")
            with list_path.open("w", encoding="utf-8") as handle:
                handle.writelines(lines)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(list_path, 0o600)
            command = (
                "ffmpeg", "-nostdin", "-v", "error", "-f", "concat", "-safe", "0",
                "-i", str(list_path), "-vn", "-ac", "1", "-ar", "16000", "-b:a", "32k",
                "-f", "mp3", "-y", str(temp_path),
            )
            process = await asyncio.create_subprocess_exec(
                *command, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
            )
            try:
                _stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=self.ffmpeg_timeout)
            except TimeoutError as exc:
                process.kill()
                await process.wait()
                raise MediaError("ffmpeg_timeout") from exc
            if process.returncode != 0 or len(stderr) > 64 * 1024 or not temp_path.is_file():
                raise MediaError("ffmpeg_failed")
            os.chmod(temp_path, 0o600)
            with temp_path.open("rb") as handle:
                os.fsync(handle.fileno())
            os.replace(temp_path, output)
            descriptor = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            list_path.unlink(missing_ok=True)
            temp_path.unlink(missing_ok=True)
