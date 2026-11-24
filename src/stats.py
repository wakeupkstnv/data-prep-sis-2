"""Tiny helper that stores per-stage record counts in data/pipeline_stats.json,
so the final verification step reads the numbers produced by real stages."""

import json
from datetime import datetime, timezone

from src.config import settings


def load_stats() -> dict:
    if settings.stats_json.exists():
        return json.loads(settings.stats_json.read_text(encoding="utf-8"))
    return {}


def update_stats(stage: str, values: dict) -> None:
    stats = load_stats()
    stats[stage] = {**values, "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    settings.stats_json.parent.mkdir(parents=True, exist_ok=True)
    settings.stats_json.write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")
