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

HTTP: httpx.AsyncClient + asyncio; a semaphore keeps at most GBIF_WORKERS (4)
requests in flight (I/O-bound work -> one event loop, no threads).

Run:  python -m src.api_client
"""

import asyncio
import json
import logging
import random

import httpx
import pandas as pd
from pydantic import ValidationError

from src.config import settings
from src.models import OccurrenceRecord
from src.stats import update_stats

log = logging.getLogger("api_client")


class GBIFRequestError(RuntimeError):
    """Raised when the API keeps failing after all retries."""


def _retry_delay(resp: httpx.Response | None, attempt: int) -> float:
    """Exponential back-off (2, 4, 8 s); a 429 with Retry-After is honoured."""
    if resp is not None and resp.status_code == 429:
        retry_after = resp.headers.get("Retry-After", "")
        if retry_after.isdigit():
            return min(float(retry_after), 60.0)
    return float(2 ** attempt)


async def get_json(params: dict, client: httpx.AsyncClient, limiter: asyncio.Semaphore) -> dict:
    """GET the endpoint with retries and exponential back-off.

    Handles: connection errors / timeouts, HTTP 429 + 5xx (retried),
    other non-200 codes (fail fast), invalid JSON bodies.
    The semaphore limits how many requests are in flight at the same time.
    """
    last_error: Exception | None = None
    for attempt in range(1, settings.gbif_max_retries + 1):
        resp: httpx.Response | None = None
        try:
            async with limiter:
                resp = await client.get(settings.gbif_base_url, params=params)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429 or resp.status_code >= 500:
                last_error = GBIFRequestError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            else:  # 4xx: request itself is wrong, retrying will not help
                raise GBIFRequestError(f"HTTP {resp.status_code} for {params}: {resp.text[:200]}")
        except httpx.TransportError as exc:  # connection refused, DNS, timeouts
            last_error = exc
        except ValueError as exc:  # body is not JSON
            last_error = GBIFRequestError(f"Invalid JSON response: {exc}")
        if attempt == settings.gbif_max_retries:
            break
        wait = _retry_delay(resp, attempt)
        log.warning("Attempt %d/%d failed (%s) - retrying in %.0fs",
                    attempt, settings.gbif_max_retries, last_error, wait)
        await asyncio.sleep(wait)  # the semaphore is released while waiting
    raise GBIFRequestError(f"Giving up after {settings.gbif_max_retries} attempts: {last_error}")


def base_params(**extra) -> dict:
    return {"country": settings.gbif_country, "hasCoordinate": "true", **extra}


async def month_counts(year: int, client: httpx.AsyncClient, limiter: asyncio.Semaphore) -> dict[int, int]:
    """Number of KZ records per month of one year (facet query, 0 records)."""
    payload = await get_json(base_params(year=year, limit=0, facet="month", facetLimit=12), client, limiter)
    counts = payload["facets"][0]["counts"] if payload.get("facets") else []
    return {int(c["name"]): int(c["count"]) for c in counts}


async def fetch_month(year: int, month: int, offset: int,
                      client: httpx.AsyncClient, limiter: asyncio.Semaphore) -> list[dict]:
    payload = await get_json(base_params(year=year, month=month, limit=settings.gbif_records_per_month,
                                         offset=offset), client, limiter)
    return payload.get("results", [])


async def plan_requests(client: httpx.AsyncClient, limiter: asyncio.Semaphore) -> list[tuple[int, int, int]]:
    """Stratified sampling plan: (year, month, random offset) for every month.

    GBIF returns results in a fixed order, so the first N rows of a query are
    not representative. We therefore split the period into year x month
    strata and start each page at a random offset (fixed seed -> the same
    plan on every run, as long as GBIF counts do not change).
    """
    years = list(range(settings.gbif_year_from, settings.gbif_year_to + 1))
    all_counts = await asyncio.gather(*(month_counts(y, client, limiter) for y in years))
    rng = random.Random(settings.gbif_random_seed)  # offsets drawn in fixed order -> reproducible
    plan = []
    for year, counts in zip(years, all_counts):
        for month in range(1, 13):
            total = counts.get(month, 0)
            if total == 0:
                continue
            upper = max(0, min(total, settings.gbif_max_random_offset) - settings.gbif_records_per_month)
            plan.append((year, month, rng.randint(0, upper)))
    return plan


async def fetch_year_facets(client: httpx.AsyncClient, limiter: asyncio.Semaphore) -> dict[int, int]:
    """Total number of KZ occurrences per year (facet query, no records)."""
    params = base_params(limit=0, facet="year", facetLimit=100,
                         year=f"{settings.gbif_year_from},{settings.gbif_year_to}")
    payload = await get_json(params, client, limiter)
    counts = payload["facets"][0]["counts"] if payload.get("facets") else []
    return {int(c["name"]): int(c["count"]) for c in counts}


def flatten(record: dict) -> dict:
    """Raw GBIF JSON -> flat dict via the Pydantic OccurrenceRecord model."""
    return OccurrenceRecord.model_validate(record).model_dump(mode="json")


async def download() -> tuple[list[tuple[int, int, int]], list[dict], list[str], dict[int, int] | None]:
    """All HTTP work: plan, records of every stratum and the yearly totals.

    One AsyncClient (connection pool) is shared by all coroutines; the
    semaphore keeps at most `gbif_workers` requests in flight so GBIF does
    not answer with HTTP 429.
    """
    limiter = asyncio.Semaphore(settings.gbif_workers)
    headers = {"User-Agent": "sis2-data-pipeline/1.0 (student project)"}
    async with httpx.AsyncClient(timeout=settings.gbif_timeout_s, headers=headers) as client:
        plan = await plan_requests(client, limiter)
        log.info("Sampling plan: %d (year, month) strata x %d records", len(plan), settings.gbif_records_per_month)

        # return_exceptions=True -> one bad stratum must not kill the whole run
        results = await asyncio.gather(*(fetch_month(y, m, off, client, limiter) for y, m, off in plan),
                                       return_exceptions=True)
        all_records: list[dict] = []
        failed_strata: list[str] = []
        for (y, m, _), res in zip(plan, results):  # gather keeps the plan order
            if isinstance(res, GBIFRequestError):
                log.error("%d-%02d skipped: %s", y, m, res)
                failed_strata.append(f"{y}-{m:02d}")
            elif isinstance(res, BaseException):
                raise res
            else:
                all_records.extend(res)
        log.info("Downloaded %d records", len(all_records))

        try:
            facets = await fetch_year_facets(client, limiter)
        except GBIFRequestError as exc:
            log.warning("facet query failed, continuing without it: %s", exc)
            facets = None
    return plan, all_records, failed_strata, facets


def collect() -> pd.DataFrame:
    settings.raw_dir.mkdir(parents=True, exist_ok=True)
    plan, all_records, failed_strata, facets = asyncio.run(download())

    if not all_records:
        raise GBIFRequestError("No records collected - check network / API availability")

    if facets is not None:
        settings.year_facets_json.write_text(json.dumps(facets, indent=2), encoding="utf-8")

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
