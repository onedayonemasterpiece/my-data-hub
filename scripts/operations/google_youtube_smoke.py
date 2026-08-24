#!/usr/bin/env python3
"""Operator-only smoke through the production analyzer and shared limiter.

This script intentionally has no direct-key/curl mode. It builds the same
fail-closed adapter used by remote MCP; every physical Google POST therefore
requires reserve -> sent -> finalize in the canonical ledger.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from uuid import uuid4

from my_data_hub.config import Settings
from my_data_hub.google_ai.analyzer import GeminiYouTubeAnalyzer, YouTubeAnalyzerConfig
from my_data_hub.google_ai.config import GoogleYouTubeSettings
from my_data_hub.google_ai.errors import GoogleAIError
from my_data_hub.google_ai.interactions import GeminiInteractionsClient
from my_data_hub.google_ai.limiter import SupabaseGoogleAILimiter


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument("--youtube-url", required=True)
    result.add_argument("--mode", choices=("summary", "transcript", "question", "custom"), default="summary")
    result.add_argument("--question")
    result.add_argument("--prompt")
    result.add_argument("--model")
    result.add_argument("--media-resolution", choices=("low", "medium", "high"))
    result.add_argument("--max-output-tokens", type=int, default=4096)
    result.add_argument("--thinking-level", choices=("minimal", "low", "medium", "high"), default="low")
    result.add_argument("--idempotency-key", default=None)
    return result


async def run(args: argparse.Namespace) -> int:
    settings = Settings.from_env(require_database=False)
    feature = GoogleYouTubeSettings.from_settings(settings)
    limiter = SupabaseGoogleAILimiter(
        supabase_url=feature.limiter_supabase_url,
        service_key=feature.limiter_supabase_service_key,
        candidate_env_names=feature.normal_key_envs,
    )
    analyzer = GeminiYouTubeAnalyzer(
        config=YouTubeAnalyzerConfig(
            enabled=feature.enabled,
            default_model=feature.model,
            allowed_models=frozenset(feature.allowed_models),
            max_output_tokens=feature.max_output_tokens,
            max_result_bytes=feature.max_response_bytes,
        ),
        limiter=limiter,
        interactions=GeminiInteractionsClient(
            timeout_seconds=feature.timeout_seconds,
            max_response_bytes=feature.max_response_bytes,
        ),
    )
    payload = {
        "youtube_url": args.youtube_url,
        "mode": args.mode,
        "question": args.question,
        "prompt": args.prompt,
        "model": args.model,
        "media_resolution": args.media_resolution,
        "max_output_tokens": args.max_output_tokens,
        "thinking_level": args.thinking_level,
        "idempotency_key": args.idempotency_key or f"operator-smoke:{uuid4()}",
    }
    payload = {key: value for key, value in payload.items() if value is not None}
    try:
        output = await analyzer.analyze(payload)
    except GoogleAIError as exc:
        print(json.dumps(exc.public(), ensure_ascii=False, sort_keys=True))
        return 2
    print(json.dumps(output, ensure_ascii=False, sort_keys=True))
    return 0


def main() -> None:
    args = parser().parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
