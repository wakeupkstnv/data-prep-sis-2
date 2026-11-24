"""Stage 1 - collect biodiversity occurrence records for Kazakhstan from the
GBIF Occurrence Search API.

Endpoint : GET https://api.gbif.org/v1/occurrence/search
Params   : country=KZ, year=<YYYY>, hasCoordinate=true, limit=<n>, offset=<k>

Sampling: the period 2015-2025 is split into year x month strata. For each
year one facet request returns the record count per month; then for every
month one page of 8 records is downloaded from a random offset (fixed seed).
11 years x 12 months x 8 = 1056 records spread over the whole decade and all
seasons (taking only the first rows would return almost only January). In addition a facet
query (facet=year, limit=0) stores the *total* number of KZ records per year.

Outputs (raw data is preserved and never modified later):
  data/raw/gbif_raw_records.jsonl  - every record exactly as returned by GBIF
  data/raw/gbif_year_counts.json   - total record count per year (facet)
  data/raw_data.csv                - flattened subset of fields used downstream

Run:  python -m src.api_client
"""

import json
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from pydantic import ValidationError

from src.config import settings
from src.models import OccurrenceRecord
from src.stats import update_stats

log = logging.getLogger("api_client")

class GBIFRequestError(RuntimeError):
    """Raised when the API keeps failing after all retries."""


def get_json(params: dict, session: requests.Session) -> dict:
    """GET the endpoint with retries and exponential back-off.

    Handles: connection errors / timeouts, HTTP 429 + 5xx (retried),
    other non-200 codes (fail fast), invalid JSON bodies.
    """
    last_error: Exception | None = None
    for attempt in range(1, settings.gbif_max_retries + 1):
        try:
            resp = session.get(settings.gbif_base_url, params=params, timeout=settings.gbif_timeout_s)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429 or resp.status_code >= 500:
                last_error = GBIFRequestError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            else:  # 4xx: request itself is wrong, retrying will not help
                raise GBIFRequestError(f"HTTP {resp.status_code} for {params}: {resp.text[:200]}")
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_error = exc
        except ValueError as exc:  # body is not JSON
            last_error = GBIFRequestError(f"Invalid JSON response: {exc}")
        wait = 2 ** attempt
        log.warning("Attempt %d/%d failed (%s) - retrying in %ds",
                    attempt, settings.gbif_max_retries, last_error, wait)
        time.sleep(wait)
    raise GBIFRequestError(f"Giving up after {settings.gbif_max_retries} attempts: {last_error}")


def base_params(**extra) -> dict:
    return {"country": settings.gbif_country, "hasCoordinate": "true", **extra}


def month_counts(year: int, session: requests.Session) -> dict[int, int]:
    """Number of KZ records per month of one year (facet query, 0 records)."""
    payload = get_json(base_params(year=year, limit=0, facet="month", facetLimit=12), session)
    counts = payload["facets"][0]["counts"] if payload.get("facets") else []
    return {int(c["name"]): int(c["count"]) for c in counts}


def fetch_month(year: int, month: int, offset: int, session: requests.Session) -> list[dict]:
    payload = get_json(base_params(year=year, month=month, limit=settings.gbif_records_per_month,
                                   offset=offset), session)
    return payload.get("results", [])


def plan_requests(session: requests.Session) -> list[tuple[int, int, int]]:
    """Stratified sampling plan: (year, month, random offset) for every month.

    GBIF returns results in a fixed order, so the first N rows of a query are
    not representative. We therefore split the period into year x month
    strata and start each page at a random offset (fixed seed -> the same
    plan on every run, as long as GBIF counts do not change).
    """
    rng = random.Random(settings.gbif_random_seed)
    plan = []
    for year in range(settings.gbif_year_from, settings.gbif_year_to + 1):
        counts = month_counts(year, session)
        for month in range(1, 13):
            total = counts.get(month, 0)
            if total == 0:
                continue
            upper = max(0, min(total, settings.gbif_max_random_offset) - settings.gbif_records_per_month)
            plan.append((year, month, rng.randint(0, upper)))
    return plan


def fetch_year_facets(session: requests.Session) -> dict[int, int]:
    """Total number of KZ occurrences per year (facet query, no records)."""
    params = base_params(limit=0, facet="year", facetLimit=100,
                         year=f"{settings.gbif_year_from},{settings.gbif_year_to}")
    payload = get_json(params, session)
    counts = payload["facets"][0]["counts"] if payload.get("facets") else []
    return {int(c["name"]): int(c["count"]) for c in counts}


def flatten(record: dict) -> dict:
    """Raw GBIF JSON -> flat dict via the Pydantic OccurrenceRecord model."""
    return OccurrenceRecord.model_validate(record).model_dump(mode="json")


def collect() -> pd.DataFrame:
    settings.raw_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = "sis2-data-pipeline/1.0 (student project)"

    plan = plan_requests(session)
    log.info("Sampling plan: %d (year, month) strata x %d records", len(plan), settings.gbif_records_per_month)

    all_records: list[dict] = []
    failed_strata: list[str] = []
    # requests are independent -> run a few in parallel; results are re-ordered by plan
    with ThreadPoolExecutor(max_workers=settings.gbif_workers) as pool:
        futures = {pool.submit(fetch_month, y, m, off, session): (y, m) for y, m, off in plan}
        by_stratum: dict[tuple[int, int], list[dict]] = {}
        for fut in as_completed(futures):
            y, m = futures[fut]
            try:
                by_stratum[(y, m)] = fut.result()
            except GBIFRequestError as exc:  # one bad stratum must not kill the whole run
                log.error("%d-%02d skipped: %s", y, m, exc)
                failed_strata.append(f"{y}-{m:02d}")
    for y, m, _ in plan:
        all_records.extend(by_stratum.get((y, m), []))
    log.info("Downloaded %d records", len(all_records))

    if not all_records:
        raise GBIFRequestError("No records collected - check network / API availability")

    try:
        facets = fetch_year_facets(session)
        settings.year_facets_json.write_text(json.dumps(facets, indent=2), encoding="utf-8")
    except GBIFRequestError as exc:
        log.warning("facet query failed, continuing without it: %s", exc)

    # 1) preserve the untouched original records
    with settings.raw_jsonl.open("w", encoding="utf-8") as fh:
        for rec in all_records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # 2) flattened raw copy (still uncleaned) for inspection and streaming
    rows, malformed = [], 0
    for rec in all_records:
        try:
            rows.append(flatten(rec))
        except ValidationError as exc:   # structurally broken record (e.g. no gbifID)
            malformed += 1
            log.warning("malformed record skipped in CSV copy: %s", exc.errors()[0]["msg"])
    df = pd.DataFrame(rows)
    df.to_csv(settings.raw_csv, index=False)

    update_stats("collection", {
        "endpoint": settings.gbif_base_url,
        "years": f"{settings.gbif_year_from}-{settings.gbif_year_to}",
        "sampling": (f"{settings.gbif_records_per_month} records per (year, month), random offset "
                     f"< {settings.gbif_max_random_offset}, seed={settings.gbif_random_seed}"),
        "strata": len(plan),
        "records_collected": len(all_records),
        "failed_strata": failed_strata,
        "malformed_records": malformed,
        "columns": len(df.columns),
    })
    log.info("Collected %d records -> %s, %s", len(all_records), settings.raw_jsonl, settings.raw_csv)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    frame = collect()
    print(f"\nTotal collected records: {len(frame)}")
    print(frame.head(3).T)
