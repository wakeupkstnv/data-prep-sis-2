# Data Dictionary

Source: GBIF Occurrence Search API — `GET https://api.gbif.org/v1/occurrence/search`
Filter: `country=KZ`, `hasCoordinate=true`, years 2015–2025.

## 1. Files produced by the pipeline

| File | Stage | Content |
|---|---|---|
| `data/raw/gbif_raw_records.jsonl` | 1 – collection | Every record exactly as GBIF returned it (one JSON object per line). Never modified. |
| `data/raw/gbif_year_counts.json` | 1 – collection | Total number of KZ records with coordinates per year (GBIF facet query). |
| `data/raw_data.csv` | 1 – collection | Flattened copy of the raw records (28 columns, still uncleaned). |
| `data/consumed_records.csv` | 2 – Kafka consumer | Records received from Kafka + `kafka_partition`, `kafka_offset`, `producer_run_id`. |
| `data/validated_records.csv` | 3 – Pandera | Records that passed every validation rule. |
| `data/invalid_records.csv` | 3 – Pandera | Quarantined records (real + test) with the column `validation_errors`. |
| `data/validation_errors.csv` | 3 – Pandera | One row per failed check: `index, column, check, failure_case, gbif_id, is_test_record`. |
| `data/processed_records.csv` | 4 – cleaning | Final clean data set (43 columns), input for SQLite. |
| `data/summaries/*.csv` | 4 – cleaning | Missing-value report, duplicate groups, region mapping, GroupBy, pivot, melt, stack tables. |
| `data/output.db` | 5 – SQLite | Tables `occurrences` and `load_runs`. |
| `data/summaries/eda_summary.txt` | 6 – EDA | Text output of the exploratory analysis. |
| `figures/*.png` | 6 – EDA | Six figures. |
| `data/pipeline_stats.json` | all | Record counts written by every stage. |
| `data/summaries/pipeline_verification.txt` | 7 | Count comparison and PASS/FAIL checks. |

## 2. Kafka message (topic `gbif-kz-occurrences`)

Key: `gbif_id` as UTF-8 bytes. Value: JSON, defined by the Pydantic model `KafkaEnvelope` (`src/models.py`).

| Field | Type | Description |
|---|---|---|
| `run_id` | string | Random id of one producer execution. |
| `seq` | int ≥ 1 | Sequence number inside the run. |
| `source` | `"gbif-occurrence-api"` | Constant. |
| `produced_at` | ISO datetime | Time the message was created. |
| `record` | object | `OccurrenceRecord` — the 28 raw fields listed in section 3. |

## 3. Raw / received columns (`raw_data.csv`, `consumed_records.csv`)

| Column | GBIF field | Type | Kind | Description |
|---|---|---|---|---|
| `gbif_id` | `gbifID` | int | identifier | Unique GBIF occurrence key. |
| `scientific_name` | `scientificName` | str | categorical | Full name with authorship. |
| `species` | `species` | str | categorical | Binomial species name (empty for genus-level IDs). |
| `kingdom`, `phylum`, `taxon_class`, `taxon_order`, `family`, `genus` | `kingdom` … `genus` | str | categorical | Taxonomic hierarchy (GBIF backbone). |
| `taxon_rank` | `taxonRank` | str | categorical | SPECIES, SUBSPECIES, GENUS, … |
| `basis_of_record` | `basisOfRecord` | str | categorical | HUMAN_OBSERVATION, PRESERVED_SPECIMEN, … |
| `occurrence_status` | `occurrenceStatus` | str | categorical | DETECTED / ABSENT. |
| `iucn_category` | `iucnRedListCategory` | str | categorical | IUCN Red List category (LC, NT, VU, EN, CR, DD, NE). |
| `state_province` | `stateProvince` | str | categorical | Region as written by the publisher (36 different spellings). |
| `latitude`, `longitude` | `decimalLatitude`, `decimalLongitude` | float | numerical | WGS84 coordinates. |
| `coord_uncertainty_m` | `coordinateUncertaintyInMeters` | float | numerical | Radius of location uncertainty. |
| `individual_count` | `individualCount` | int | numerical | Number of individuals seen. |
| `event_date` | `eventDate` | str | date/time | ISO date, date-time or range `start/end`. |
| `year`, `month`, `day` | same | int | date/time | Date parts. |
| `dataset_name`, `institution_code` | `datasetName`, `institutionCode` | str | categorical | Data source (eBird, iNaturalist, …). |
| `license` | `license` | str | categorical | Creative Commons licence URL. |
| `last_interpreted` | `lastInterpreted` | str | date/time | When GBIF last processed the record. |
| `issue_count` | `len(issues)` | int | numerical | Number of GBIF data-quality flags. |
| `media_count` | `len(media)` | int | numerical | Number of attached photos/sounds. |

## 4. Table `occurrences` in `data/output.db` (= `processed_records.csv`)

Primary key: `gbif_id`. Unique index: `(scientific_name, latitude, longitude, event_date)`.

| Column | SQLite type | Description / how it was derived |
|---|---|---|
| `gbif_id` | INTEGER PK | GBIF occurrence key. |
| `scientific_name` | TEXT | Whitespace-normalized. |
| `species` | TEXT | Missing species → `"<Genus> sp."`. |
| `genus`, `family`, `taxon_order`, `taxon_class`, `phylum`, `kingdom` | TEXT | Title case; missing → `"Unknown"`. |
| `taxon_rank` | TEXT | Upper case; missing → `UNRANKED`. |
| `identified_to_species` | INTEGER 0/1 | 1 if the original `species` was present. |
| `basis_of_record` | TEXT | Upper case code. |
| `iucn_category` | TEXT | Missing → `NE` (Not Evaluated). CHECK constraint on allowed values. |
| `is_threatened` | INTEGER 0/1 | `iucn_category ∈ {VU, EN, CR}`. |
| `region` | TEXT | Canonical region (16 values incl. `Unknown`), see cleaner.py. |
| `region_source` | TEXT | `reported` / `nearest_neighbour` (filled from the closest labelled record ≤ 50 km) / `unknown`. |
| `state_province_raw` | TEXT | Original spelling (for traceability). |
| `latitude`, `longitude` | REAL | CHECK inside the Kazakhstan bounding box. |
| `coord_uncertainty_m` | REAL | Missing → median of the same `basis_of_record`. |
| `uncertainty_imputed` | INTEGER 0/1 | 1 if the value above was imputed. |
| `log10_uncertainty` | REAL | `log10(uncertainty + 1)` (NumPy). |
| `precision_class` | TEXT | high (≤100 m) / medium (≤1 km) / low (>1 km). |
| `individual_count` | INTEGER | Missing → 1 (a presence record means at least one individual). |
| `count_imputed` | INTEGER 0/1 | 1 if the value above was imputed. |
| `event_date` | TEXT | `YYYY-MM-DD` (start of the range if GBIF gave a range). |
| `year`, `month`, `day`, `day_of_year` | INTEGER | Date parts. |
| `season` | TEXT | Winter (Dec–Feb), Spring, Summer, Autumn (NumPy `np.select`). |
| `doy_sin`, `doy_cos` | REAL | Cyclic encoding of day of year. |
| `dist_center_km`, `dist_almaty_km`, `dist_astana_km` | REAL | Haversine distance (NumPy) to the centre of Kazakhstan / Almaty / Astana. |
| `dist_almaty_z` | REAL | z-score of `dist_almaty_km`. |
| `dataset_name`, `institution_code` | TEXT | Missing → `Unknown dataset` / `Unknown`. |
| `license` | TEXT | Short code: CC-BY, CC-BY-NC, CC0, OTHER. |
| `issue_count`, `media_count` | INTEGER | As received. |
| `is_high_quality` | INTEGER 0/1 | uncertainty ≤ 1 km AND ≤ 3 GBIF issues AND identified to species. |
| `row_hash` | TEXT | SHA-256 of all business columns — used for idempotent upserts. |
| `first_loaded_at`, `last_updated_at` | TEXT | UTC timestamps. |

## 5. Table `load_runs`

One row per execution of the loading stage: `run_id, started_at, source_file, rows_in_file, inserted, updated, unchanged, rows_in_table`.
