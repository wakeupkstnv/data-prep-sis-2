"""Stage 3 - data validation with Pandera.

Input : data/consumed_records.csv (exactly what the Kafka consumer received)
Output: data/validated_records.csv  - rows that passed every rule
        data/invalid_records.csv    - quarantined rows + their error messages
        data/validation_errors.csv  - one line per (row, column, rule) failure

The schema is applied BEFORE cleaning, on the received data. Pandera runs in
lazy mode, so all failures of all rows are collected in one pass instead of
stopping at the first error.

Handling strategy: invalid rows are *quarantined* (written to a separate file
with the reasons) and excluded from further processing - we never silently
"repair" a record whose taxonomy, location or date is wrong. Missing values in
optional columns are allowed here and handled later in cleaner.py.

Validation testing: three deliberately invalid test records (gbif_id < 0,
dataset_name = "SIS2_VALIDATION_TEST", is_test_record = True) are appended so
the report can show that each rule really fires. They always end up in the
invalid file and are additionally filtered out by the flag as a safety net.

Run:  python -m src.validator            (with test records)
      python -m src.validator --no-test-records
"""

import argparse
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors

from src.config import settings
from src.stats import update_stats

log = logging.getLogger("validator")

CURRENT_YEAR = datetime.now(timezone.utc).year

# Kazakhstan bounding box (with a small margin)
KZ_LAT = (40.5, 55.5)
KZ_LON = (46.4, 87.4)

ALLOWED_KINGDOMS = ["Animalia", "Plantae", "Fungi", "Bacteria", "Chromista", "Protozoa", "Archaea", "Viruses"]
ALLOWED_BASIS = ["HUMAN_OBSERVATION", "MACHINE_OBSERVATION", "OBSERVATION", "PRESERVED_SPECIMEN",
                 "FOSSIL_SPECIMEN", "LIVING_SPECIMEN", "MATERIAL_SAMPLE", "MATERIAL_CITATION", "OCCURRENCE"]
ALLOWED_IUCN = ["LC", "NT", "VU", "EN", "CR", "EW", "EX", "DD", "NE"]
ALLOWED_RANKS = ["SPECIES", "SUBSPECIES", "VARIETY", "FORM", "GENUS", "FAMILY", "ORDER", "CLASS",
                 "PHYLUM", "KINGDOM", "UNRANKED"]
MAX_UNCERTAINTY_M = 50_000  # > 50 km cannot be placed reliably inside one region


def parse_event_date(values: pd.Series) -> pd.Series:
    """GBIF eventDate may be '2020-05-01', '2020-05-01T10:00Z' or a range
    '2020-05-01/2020-05-31' -> take the start, parse as UTC."""
    start = values.astype("string").str.split("/").str[0]
    return pd.to_datetime(start, errors="coerce", utc=True, format="ISO8601")


def _event_date_parseable(s: pd.Series) -> pd.Series:
    return parse_event_date(s).notna()


def _event_date_not_in_future(s: pd.Series) -> pd.Series:
    parsed = parse_event_date(s)
    return parsed.isna() | (parsed <= pd.Timestamp.now(tz="UTC"))


def _year_matches_event_date(df: pd.DataFrame) -> pd.Series:
    parsed_year = parse_event_date(df["event_date"]).dt.year
    return parsed_year.isna() | df["year"].isna() | (parsed_year == df["year"])


def build_schema() -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        columns={
            # R1 data type + required identifier
            "gbif_id": pa.Column("int64", pa.Check.gt(0, error="gbif_id must be positive"),
                                 nullable=False, coerce=True),
            # R2 required taxonomic fields
            "scientific_name": pa.Column(str, pa.Check.str_length(min_value=3), nullable=False),
            # R3 allowed categorical values
            "kingdom": pa.Column(str, pa.Check.isin(ALLOWED_KINGDOMS), nullable=False),
            "basis_of_record": pa.Column(str, pa.Check.isin(ALLOWED_BASIS), nullable=False),
            "taxon_rank": pa.Column(str, pa.Check.isin(ALLOWED_RANKS), nullable=True),
            "iucn_category": pa.Column(str, pa.Check.isin(ALLOWED_IUCN), nullable=True),
            # R4 numeric ranges - the point must lie inside Kazakhstan
            "latitude": pa.Column(float, pa.Check.in_range(*KZ_LAT, error="latitude outside Kazakhstan"),
                                  nullable=False, coerce=True),
            "longitude": pa.Column(float, pa.Check.in_range(*KZ_LON, error="longitude outside Kazakhstan"),
                                   nullable=False, coerce=True),
            "coord_uncertainty_m": pa.Column(float, pa.Check.in_range(0, MAX_UNCERTAINTY_M,
                                             error=f"uncertainty must be 0..{MAX_UNCERTAINTY_M} m"),
                                             nullable=True, coerce=True),
            "individual_count": pa.Column(float, pa.Check.in_range(1, 10_000), nullable=True, coerce=True),
            # R5 date/time validation
            "year": pa.Column("Int64", pa.Check.in_range(1900, CURRENT_YEAR), nullable=False, coerce=True),
            "month": pa.Column("Int64", pa.Check.in_range(1, 12), nullable=True, coerce=True),
            "day": pa.Column("Int64", pa.Check.in_range(1, 31), nullable=True, coerce=True),
            "event_date": pa.Column(str, [
                pa.Check(_event_date_parseable, error="event_date is not a valid ISO date"),
                pa.Check(_event_date_not_in_future, error="event_date is in the future"),
            ], nullable=False),
        },
        # R6 custom cross-column rule
        checks=[pa.Check(_year_matches_event_date, error="year does not match event_date")],
        strict=False,  # extra columns (dataset_name, kafka_offset, ...) are allowed
        name="GBIFOccurrenceSchema",
    )


def make_test_records(template: pd.DataFrame) -> pd.DataFrame:
    """Three deliberately invalid records, each breaking several rules."""
    base = template.iloc[0].to_dict()
    tests = [
        {**base, "gbif_id": -1, "latitude": 95.0, "longitude": 10.0,            # out of range / not KZ
         "kingdom": "Aliens", "coord_uncertainty_m": -5,                        # bad category, negative
         "year": int(base["year"]) - 3},                                        # year != event_date year
        {**base, "gbif_id": -2, "scientific_name": None, "basis_of_record": "GUESS",
         "event_date": "2999-01-01", "year": 2999, "month": 13},                 # missing name, future date
        {**base, "gbif_id": -3, "event_date": "not-a-date", "year": 1700,
         "individual_count": "many", "iucn_category": "XX"},                    # wrong type & format
    ]
    df = pd.DataFrame(tests)
    df["dataset_name"] = "SIS2_VALIDATION_TEST"
    df["is_test_record"] = True
    return df


def validate(add_test_records: bool = True) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not settings.consumed_csv.exists():
        raise FileNotFoundError(f"{settings.consumed_csv} not found - run the consumer first")
    received = pd.read_csv(settings.consumed_csv)
    received["is_test_record"] = False
    n_received = len(received)

    frames = [received]
    if add_test_records:
        frames.append(make_test_records(received))
    df = pd.concat(frames, ignore_index=True)
    # keep everything as 'object' before validation so that one bad value
    # (e.g. latitude="abc") is reported by Pandera instead of crashing read_csv
    df = df.astype(object).where(df.notna(), None)

    schema = build_schema()
    try:
        schema.validate(df, lazy=True)
        failures = pd.DataFrame(columns=["index", "column", "check", "failure_case"])
    except SchemaErrors as err:
        failures = err.failure_cases[["index", "column", "check", "failure_case"]].copy()

    # dataframe-level checks report index; coerce errors may report index=None
    failures = failures.dropna(subset=["index"])
    failures["index"] = failures["index"].astype(int)
    failures["gbif_id"] = df.loc[failures["index"], "gbif_id"].to_numpy()
    failures["is_test_record"] = df.loc[failures["index"], "is_test_record"].to_numpy()
    failures["column"] = failures["column"].fillna("<row>")
    # a DataFrame-level check is reported once per column -> collapse to one row-level failure
    row_level = failures["check"].isin([c.error for c in schema.checks])
    failures.loc[row_level, "column"] = "<row>"
    failures.loc[row_level, "failure_case"] = "year=" + df.loc[failures.loc[row_level, "index"], "year"].astype(str).to_numpy()
    failures = failures.drop_duplicates(subset=["index", "column", "check"]).reset_index(drop=True)

    invalid_idx = np.unique(failures["index"].to_numpy())
    reasons = (failures.assign(reason=failures["column"].astype(str) + ": " + failures["check"].astype(str))
               .groupby("index")["reason"].apply(lambda r: " | ".join(sorted(set(r)))))

    invalid = df.loc[invalid_idx].copy()
    invalid["validation_errors"] = reasons.reindex(invalid_idx).to_numpy()
    valid = df.drop(index=invalid_idx)
    valid = valid[~valid["is_test_record"].astype(bool)]  # safety net: test rows never pass on

    settings.data_dir.mkdir(parents=True, exist_ok=True)
    valid.to_csv(settings.valid_csv, index=False)
    invalid.to_csv(settings.invalid_csv, index=False)
    failures.to_csv(settings.validation_errors_csv, index=False)

    real_invalid = invalid[~invalid["is_test_record"].astype(bool)]
    test_invalid = invalid[invalid["is_test_record"].astype(bool)]
    rule_counts = failures[~failures["is_test_record"].astype(bool)].groupby(["column", "check"]).size()
    update_stats("validation", {
        "records_received": n_received,
        "test_records_added": int(df["is_test_record"].sum()),
        "rows_validated": len(df),
        "valid_records": len(valid),
        "invalid_real_records": len(real_invalid),
        "invalid_test_records_detected": len(test_invalid),
        "failures_total": len(failures),
        "failures_by_rule_real_data": {f"{c} :: {k}": int(v) for (c, k), v in rule_counts.items()},
        "handling_strategy": "quarantine invalid rows to invalid_records.csv; continue with valid rows",
    })
    return valid, invalid, failures


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Validate received records with Pandera")
    parser.add_argument("--no-test-records", action="store_true")
    args = parser.parse_args()
    valid, invalid, failures = validate(add_test_records=not args.no_test_records)

    test_mask = invalid["is_test_record"].astype(bool)
    print("\n=== VALIDATION SUMMARY ===")
    print(f"valid records            : {len(valid)}")
    print(f"invalid real records     : {int((~test_mask).sum())}")
    print(f"invalid TEST records     : {int(test_mask.sum())} (all excluded)")
    print(f"total rule failures      : {len(failures)}")
    print("\nFailures per rule (real data only):")
    real = failures[~failures["is_test_record"].astype(bool)]
    print(real.groupby(["column", "check"]).size().to_string() if len(real) else "  none")
    print("\nErrors detected in the deliberately invalid test records:")
    for _, row in invalid[test_mask].iterrows():
        print(f"  gbif_id={row['gbif_id']}: {row['validation_errors']}")


if __name__ == "__main__":
    main()
