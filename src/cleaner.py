"""Stage 4 - cleaning and transformation with pandas + NumPy.

Input : data/validated_records.csv (only rows that passed the Pandera schema)
Output: data/processed_records.csv  - one clean row per occurrence (goes to SQLite)
        data/summaries/*.csv        - missing-value report, group-by tables, pivots

Steps (each one is explained in the report):
  1. missing-value analysis
  2. duplicate detection: exact gbif_id + "same observation" duplicates
  3. type conversion (datetime, Int64, category, float)
  4. text standardization: whitespace, case, region names (36 spellings -> 17 regions)
  5. missing-value handling (fill / impute with flags / nearest-neighbour region)
  6. NumPy feature engineering (haversine distances, log scale, seasons, z-scores)
  7. multi-condition filtering
  8. GroupBy + aggregation
  9. reshaping (pivot_table, melt, stack/unstack)

Run:  python -m src.cleaner
"""

import logging
import re
import unicodedata

import numpy as np
import pandas as pd

from src.config import settings
from src.stats import update_stats

log = logging.getLogger("cleaner")

EARTH_RADIUS_KM = 6371.0
KZ_CENTER = (48.0196, 66.9237)   # geographic centre of Kazakhstan
ALMATY = (43.2389, 76.8897)
ASTANA = (51.1694, 71.4491)
THREATENED = ["VU", "EN", "CR"]  # IUCN "threatened" categories
ALMATY_CITY_RADIUS_KM = 20       # "Almaty" labels are used for both the city and the oblast
NN_MAX_DISTANCE_KM = 50          # nearest-neighbour region fill only if a labelled point is this close

# canonical region <- regex on the ASCII-folded, lower-cased province text
REGION_PATTERNS = [
    (r"almat|алмат", "Almaty Region"),   # split into city / region by coordinates below
    (r"astana|nur-sultan", "Astana City"),
    (r"shymkent", "Shymkent City"),
    (r"aqmola|akmol|акмол", "Akmola Region"),
    (r"aqtobe|aktyub|aktobe|актюб|актоб", "Aktobe Region"),
    (r"atyrau|атырау", "Atyrau Region"),
    (r"batys|west kaz|zapadno|западно", "West Kazakhstan Region"),
    (r"shyghys|east kaz|vostochno|восточно", "East Kazakhstan Region"),
    (r"soltustik|north kaz|severo|северо", "North Kazakhstan Region"),
    (r"ongtustik|south kaz|yuzhno|turkist|южно|туркест", "Turkistan Region"),
    (r"mang|мангист", "Mangystau Region"),
    (r"qaragh|karag|караг", "Karaganda Region"),
    (r"zhambyl|jambyl|zhambyl|жамбыл", "Zhambyl Region"),
    (r"qyzylorda|kyzylord|кызылорд", "Kyzylorda Region"),
    (r"qostanay|kostanay|костана", "Kostanay Region"),
    (r"pavlodar|павлодар", "Pavlodar Region"),
    (r"abai|abay|абай", "Abai Region"),
    (r"zhetisu|жетіс|жетыс", "Zhetisu Region"),
    (r"ulytau|улытау", "Ulytau Region"),
]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def fold_text(value: str) -> str:
    """lower-case, strip accents (ü -> u), collapse whitespace. Cyrillic is kept."""
    value = unicodedata.normalize("NFKD", str(value))
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", value).strip().lower()


def standardize_region(raw: str | float) -> str | float:
    if pd.isna(raw):
        return np.nan
    folded = fold_text(raw)
    for pattern, canonical in REGION_PATTERNS:
        if re.search(pattern, folded):
            return canonical
    return np.nan


def haversine_km(lat1, lon1, lat2, lon2) -> np.ndarray:
    """Vectorised great-circle distance (NumPy broadcasting works for matrices too)."""
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


def save_summary(df: pd.DataFrame, name: str, index: bool = True) -> None:
    settings.summary_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(settings.summary_dir / f"{name}.csv", index=index)


# ---------------------------------------------------------------------------
# steps
# ---------------------------------------------------------------------------
def missing_value_report(df: pd.DataFrame) -> pd.DataFrame:
    report = pd.DataFrame({"missing": df.isna().sum(), "missing_pct": (df.isna().mean() * 100).round(2)})
    report = report[report["missing"] > 0].sort_values("missing", ascending=False)
    save_summary(report, "missing_values_before_cleaning")
    return report


def remove_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    n0 = len(df)
    # (a) exact technical duplicates - same GBIF id (e.g. a Kafka topic replayed twice)
    df = df.drop_duplicates(subset="gbif_id", keep="first")
    n_id = n0 - len(df)
    # (b) the same observation published twice (same taxon, place, day) - e.g. an
    #     observer uploads the same bird to eBird and to iNaturalist. Keep the
    #     most precise copy (smallest coordinate uncertainty, then most media).
    key = ["scientific_name", "lat_r", "lon_r", "event_day"]
    df = df.assign(lat_r=df["latitude"].round(4), lon_r=df["longitude"].round(4),
                   event_day=df["event_date"].dt.date)
    dup_mask = df.duplicated(subset=key, keep=False)
    save_summary(df.loc[dup_mask, ["gbif_id", *key, "dataset_name", "coord_uncertainty_m"]]
                 .sort_values(key), "duplicate_groups", index=False)
    df = (df.sort_values(["coord_uncertainty_m", "media_count"], ascending=[True, False], na_position="last")
            .drop_duplicates(subset=key, keep="first")
            .sort_values("gbif_id")
            .drop(columns=["lat_r", "lon_r", "event_day"]))
    n_sem = n0 - n_id - len(df)
    return df, {"duplicate_gbif_ids_removed": n_id, "same_observation_duplicates_removed": n_sem,
                "rows_in_duplicate_groups": int(dup_mask.sum())}


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["gbif_id"] = df["gbif_id"].astype("int64")
    start = df["event_date"].astype("string").str.split("/").str[0]          # ranges -> start date
    df["event_date"] = pd.to_datetime(start, utc=True, format="ISO8601").dt.tz_localize(None)
    for col in ["year", "month", "day", "individual_count", "issue_count", "media_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ["latitude", "longitude", "coord_uncertainty_m"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    df["last_interpreted"] = pd.to_datetime(df["last_interpreted"], utc=True, format="ISO8601")
    return df


def normalize_text(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    text_cols = ["scientific_name", "species", "kingdom", "phylum", "taxon_class", "taxon_order",
                 "family", "genus", "dataset_name", "institution_code", "state_province"]
    for col in text_cols:
        df[col] = df[col].astype("string").str.strip().str.replace(r"\s+", " ", regex=True)
    for col in ["kingdom", "phylum", "taxon_class", "taxon_order", "family", "genus"]:
        df[col] = df[col].str.title()
    for col in ["basis_of_record", "taxon_rank", "iucn_category"]:
        df[col] = df[col].astype("string").str.strip().str.upper()
    df["license"] = (df["license"].astype("string")
                     .str.extract(r"licenses/([a-z\-]+)/", expand=False).str.upper()
                     .str.replace("BY", "CC-BY", n=1).fillna("OTHER"))
    df["region"] = df["state_province"].map(standardize_region).astype("string")
    # GBIF publishers write "Almaty" both for the city and for the oblast (points up to
    # ~500 km away), so the city is separated geometrically: within 20 km of its centre.
    in_city = haversine_km(df["latitude"], df["longitude"], *ALMATY) <= ALMATY_CITY_RADIUS_KM
    df.loc[(df["region"] == "Almaty Region") & in_city, "region"] = "Almaty City"
    mapping = (df.dropna(subset=["state_province"])
                 .groupby(["region", "state_province"], dropna=False).size()
                 .rename("records").reset_index()
                 .sort_values(["region", "records"], ascending=[True, False]))
    save_summary(mapping, "region_name_mapping", index=False)
    return df, mapping


def handle_missing(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    df = df.copy()
    info: dict = {}

    # region: nearest labelled neighbour (NumPy distance matrix) within 50 km
    known = df[df["region"].notna()]
    unknown_idx = df.index[df["region"].isna()]
    df["region_source"] = np.where(df["region"].notna(), "reported", "unknown")
    if len(unknown_idx) and len(known):
        dist = haversine_km(df.loc[unknown_idx, "latitude"].to_numpy()[:, None],
                            df.loc[unknown_idx, "longitude"].to_numpy()[:, None],
                            known["latitude"].to_numpy()[None, :],
                            known["longitude"].to_numpy()[None, :])      # shape (n_unknown, n_known)
        nearest = dist.argmin(axis=1)
        close = dist[np.arange(len(unknown_idx)), nearest] <= NN_MAX_DISTANCE_KM
        fill_idx = unknown_idx[close]
        df.loc[fill_idx, "region"] = known["region"].to_numpy()[nearest[close]]
        df.loc[fill_idx, "region_source"] = "nearest_neighbour"
        info["region_filled_by_nearest_neighbour"] = int(close.sum())
    df["region"] = df["region"].fillna("Unknown")
    info["region_still_unknown"] = int((df["region"] == "Unknown").sum())

    # species: genus-level identifications have no species -> "Genus sp."
    df["identified_to_species"] = df["species"].notna()
    df["species"] = df["species"].fillna(df["genus"] + " sp.").fillna(df["scientific_name"])
    info["species_filled_as_genus_sp"] = int((~df["identified_to_species"]).sum())

    # coordinate uncertainty: median of the same dataset/basis group, flagged
    df["uncertainty_imputed"] = df["coord_uncertainty_m"].isna()
    group_median = df.groupby("basis_of_record")["coord_uncertainty_m"].transform("median")
    df["coord_uncertainty_m"] = df["coord_uncertainty_m"].fillna(group_median)
    df["coord_uncertainty_m"] = df["coord_uncertainty_m"].fillna(df["coord_uncertainty_m"].median())
    info["coord_uncertainty_imputed"] = int(df["uncertainty_imputed"].sum())

    # individual count: a presence record means at least one individual was seen
    df["count_imputed"] = df["individual_count"].isna()
    df["individual_count"] = df["individual_count"].fillna(1)
    info["individual_count_imputed"] = int(df["count_imputed"].sum())

    # categorical gaps
    df["iucn_category"] = df["iucn_category"].fillna("NE")            # NE = Not Evaluated
    for col in ["taxon_class", "taxon_order", "family", "genus", "phylum"]:
        df[col] = df[col].fillna("Unknown")
    df["dataset_name"] = df["dataset_name"].fillna("Unknown dataset")
    df["institution_code"] = df["institution_code"].fillna("Unknown")
    df["taxon_rank"] = df["taxon_rank"].fillna("UNRANKED")
    # day missing -> event_date has month precision; recompute from parsed date
    df["day"] = df["day"].fillna(df["event_date"].dt.day.astype("Int64"))
    info["missing_after_cleaning"] = int(df.drop(columns=["state_province"]).isna().sum().sum())
    return df, info


def add_numpy_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    lat, lon = df["latitude"].to_numpy(), df["longitude"].to_numpy()
    df["dist_center_km"] = np.round(haversine_km(lat, lon, *KZ_CENTER), 1)
    df["dist_almaty_km"] = np.round(haversine_km(lat, lon, *ALMATY), 1)
    df["dist_astana_km"] = np.round(haversine_km(lat, lon, *ASTANA), 1)
    df["log10_uncertainty"] = np.round(np.log10(df["coord_uncertainty_m"].to_numpy() + 1), 3)
    month = df["month"].to_numpy(dtype=float)
    df["season"] = np.select([np.isin(month, [12, 1, 2]), np.isin(month, [3, 4, 5]),
                              np.isin(month, [6, 7, 8])], ["Winter", "Spring", "Summer"], default="Autumn")
    df["day_of_year"] = df["event_date"].dt.day_of_year.astype("int64")
    # cyclic encoding of the season (Dec 31 is next to Jan 1)
    df["doy_sin"] = np.round(np.sin(2 * np.pi * df["day_of_year"] / 365.25), 4)
    df["doy_cos"] = np.round(np.cos(2 * np.pi * df["day_of_year"] / 365.25), 4)
    unc = df["coord_uncertainty_m"].to_numpy()
    df["precision_class"] = np.where(unc <= 100, "high (<=100 m)",
                                     np.where(unc <= 1000, "medium (<=1 km)", "low (>1 km)"))
    d = df["dist_almaty_km"].to_numpy()
    df["dist_almaty_z"] = np.round((d - d.mean()) / d.std(), 3)
    df["is_threatened"] = np.isin(df["iucn_category"].to_numpy(), THREATENED)
    df["is_high_quality"] = (unc <= 1000) & (df["issue_count"].to_numpy() <= 3) & df["identified_to_species"].to_numpy()
    return df


def finalize_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["kingdom", "taxon_class", "basis_of_record", "taxon_rank", "iucn_category",
                "region", "region_source", "season", "precision_class", "license"]:
        df[col] = df[col].astype("category")
    df["individual_count"] = df["individual_count"].astype("int64")
    return df


def filter_examples(df: pd.DataFrame) -> dict:
    """Multi-condition filters used in the analysis."""
    threatened_precise_recent = df[df["is_threatened"] & (df["coord_uncertainty_m"] <= 1000) & (df["year"] >= 2020)]
    summer_birds_outside_almaty = df[(df["taxon_class"] == "Aves") & (df["season"] == "Summer")
                                     & (df["dist_almaty_km"] > 300)]
    plants_with_photos = df[(df["kingdom"] == "Plantae") & (df["media_count"] > 0) & df["is_high_quality"]]
    save_summary(threatened_precise_recent[["gbif_id", "species", "iucn_category", "region",
                                            "event_date", "coord_uncertainty_m"]],
                 "filter_threatened_precise_since_2020", index=False)
    return {"threatened & uncertainty<=1km & year>=2020": len(threatened_precise_recent),
            "Aves & Summer & >300 km from Almaty": len(summer_birds_outside_almaty),
            "Plantae & has photo & high quality": len(plants_with_photos)}


def group_and_reshape(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    by_class = (df.groupby("taxon_class", observed=True)
                  .agg(records=("gbif_id", "count"), species=("species", "nunique"),
                       threatened=("is_threatened", "sum"),
                       median_uncertainty_m=("coord_uncertainty_m", "median"),
                       mean_dist_almaty_km=("dist_almaty_km", "mean"))
                  .sort_values("records", ascending=False).round(1))
    by_region = (df.groupby("region", observed=True)
                   .agg(records=("gbif_id", "count"), species=("species", "nunique"),
                        kingdoms=("kingdom", "nunique"), threatened_share=("is_threatened", "mean"),
                        first_year=("year", "min"), last_year=("year", "max"))
                   .assign(threatened_share=lambda t: (t["threatened_share"] * 100).round(1))
                   .sort_values("records", ascending=False))
    by_year_basis = df.groupby(["year", "basis_of_record"], observed=True).size().unstack(fill_value=0)

    # pivot_table: season x kingdom record counts
    season_kingdom = pd.pivot_table(df, index="season", columns="kingdom", values="gbif_id",
                                    aggfunc="count", fill_value=0, observed=True)
    season_kingdom = season_kingdom.reindex(["Winter", "Spring", "Summer", "Autumn"])
    # pivot_table: month x top-5 class
    top_classes = df["taxon_class"].value_counts().head(5).index
    month_class = pd.pivot_table(df[df["taxon_class"].isin(top_classes)], index="month",
                                 columns="taxon_class", values="gbif_id", aggfunc="count",
                                 fill_value=0, observed=True)
    month_class.columns = month_class.columns.astype(str)
    # melt: wide month x class table back to long format (tidy data for seaborn)
    month_class_long = month_class.reset_index().melt(id_vars="month", var_name="taxon_class",
                                                      value_name="records")
    # stack: region x season matrix -> long Series
    region_season = pd.crosstab(df["region"], df["season"])
    region_season_long = region_season.stack().rename("records").reset_index()

    tables = {"groupby_taxon_class": by_class, "groupby_region": by_region,
              "groupby_year_basis_unstack": by_year_basis, "pivot_season_kingdom": season_kingdom,
              "pivot_month_top_classes": month_class, "melt_month_top_classes": month_class_long,
              "stack_region_season": region_season_long}
    for name, table in tables.items():
        save_summary(table, name, index=name not in {"melt_month_top_classes", "stack_region_season"})
    return tables


OUTPUT_COLUMNS = [
    "gbif_id", "scientific_name", "species", "genus", "family", "taxon_order", "taxon_class", "phylum",
    "kingdom", "taxon_rank", "identified_to_species", "basis_of_record", "iucn_category", "is_threatened",
    "region", "region_source", "state_province", "latitude", "longitude", "coord_uncertainty_m",
    "uncertainty_imputed", "log10_uncertainty", "precision_class", "individual_count", "count_imputed",
    "event_date", "year", "month", "day", "day_of_year", "season", "doy_sin", "doy_cos",
    "dist_center_km", "dist_almaty_km", "dist_astana_km", "dist_almaty_z", "dataset_name",
    "institution_code", "license", "issue_count", "media_count", "is_high_quality",
]


def clean() -> pd.DataFrame:
    if not settings.valid_csv.exists():
        raise FileNotFoundError(f"{settings.valid_csv} not found - run the validator first")
    df = pd.read_csv(settings.valid_csv)
    n_in = len(df)
    log.info("Loaded %d validated records", n_in)

    missing = missing_value_report(df)
    log.info("Columns with missing values:\n%s", missing.to_string())
    df = convert_types(df)
    df, dup_info = remove_duplicates(df)
    log.info("Duplicates: %s", dup_info)
    df, mapping = normalize_text(df)
    log.info("Region names: %d raw spellings -> %d canonical regions",
             mapping["state_province"].nunique(), mapping["region"].nunique())
    df, missing_info = handle_missing(df)
    log.info("Missing-value handling: %s", missing_info)
    df = add_numpy_features(df)
    df = finalize_types(df)
    filters = filter_examples(df)
    tables = group_and_reshape(df)

    out = df[OUTPUT_COLUMNS].copy()
    out["event_date"] = out["event_date"].dt.strftime("%Y-%m-%d")
    out.to_csv(settings.processed_csv, index=False)

    update_stats("cleaning", {
        "records_in": n_in, "records_out": len(out), **dup_info,
        "raw_region_spellings": int(mapping["state_province"].nunique()),
        "canonical_regions": int(out["region"].nunique()), **missing_info,
        "filters": filters, "columns_out": len(OUTPUT_COLUMNS),
    })
    print("\n=== CLEANING SUMMARY ===")
    print(f"records in / out          : {n_in} / {len(out)}")
    print(f"duplicates removed        : {dup_info}")
    print(f"missing-value handling    : {missing_info}")
    print(f"multi-condition filters   : {filters}")
    print("\nGroupBy taxon_class (top 8):\n", tables["groupby_taxon_class"].head(8).to_string())
    print("\npivot_table season x kingdom:\n", tables["pivot_season_kingdom"].to_string())
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    clean()
