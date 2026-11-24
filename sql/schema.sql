-- SQLite3 schema for processed GBIF occurrences (Kazakhstan)
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS occurrences (
    gbif_id               INTEGER PRIMARY KEY,               -- GBIF occurrence key = natural key
    scientific_name       TEXT    NOT NULL,
    species               TEXT    NOT NULL,
    genus                 TEXT    NOT NULL,
    family                TEXT    NOT NULL,
    taxon_order           TEXT    NOT NULL,
    taxon_class           TEXT    NOT NULL,
    phylum                TEXT    NOT NULL,
    kingdom               TEXT    NOT NULL,
    taxon_rank            TEXT    NOT NULL,
    identified_to_species INTEGER NOT NULL CHECK (identified_to_species IN (0, 1)),
    basis_of_record       TEXT    NOT NULL,
    iucn_category         TEXT    NOT NULL CHECK (iucn_category IN ('LC','NT','VU','EN','CR','EW','EX','DD','NE')),
    is_threatened         INTEGER NOT NULL CHECK (is_threatened IN (0, 1)),
    region                TEXT    NOT NULL,
    region_source         TEXT    NOT NULL CHECK (region_source IN ('reported','nearest_neighbour','unknown')),
    state_province_raw    TEXT,                                -- original spelling, kept for traceability
    latitude              REAL    NOT NULL CHECK (latitude  BETWEEN 40.5 AND 55.5),
    longitude             REAL    NOT NULL CHECK (longitude BETWEEN 46.4 AND 87.4),
    coord_uncertainty_m   REAL    NOT NULL CHECK (coord_uncertainty_m >= 0),
    uncertainty_imputed   INTEGER NOT NULL CHECK (uncertainty_imputed IN (0, 1)),
    log10_uncertainty     REAL    NOT NULL,
    precision_class       TEXT    NOT NULL,
    individual_count      INTEGER NOT NULL CHECK (individual_count >= 1),
    count_imputed         INTEGER NOT NULL CHECK (count_imputed IN (0, 1)),
    event_date            TEXT    NOT NULL,                    -- ISO 8601 YYYY-MM-DD
    year                  INTEGER NOT NULL,
    month                 INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    day                   INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
    day_of_year           INTEGER NOT NULL,
    season                TEXT    NOT NULL CHECK (season IN ('Winter','Spring','Summer','Autumn')),
    doy_sin               REAL    NOT NULL,
    doy_cos               REAL    NOT NULL,
    dist_center_km        REAL    NOT NULL,
    dist_almaty_km        REAL    NOT NULL,
    dist_astana_km        REAL    NOT NULL,
    dist_almaty_z         REAL    NOT NULL,
    dataset_name          TEXT    NOT NULL,
    institution_code      TEXT    NOT NULL,
    license               TEXT    NOT NULL,
    issue_count           INTEGER NOT NULL,
    media_count           INTEGER NOT NULL,
    is_high_quality       INTEGER NOT NULL CHECK (is_high_quality IN (0, 1)),
    row_hash              TEXT    NOT NULL,                    -- SHA-256 of the business columns
    first_loaded_at       TEXT    NOT NULL,
    last_updated_at       TEXT    NOT NULL
);

-- one physical observation must not be stored twice under different ids
CREATE UNIQUE INDEX IF NOT EXISTS ux_occurrence_observation
    ON occurrences (scientific_name, latitude, longitude, event_date);
CREATE INDEX IF NOT EXISTS ix_occurrences_class  ON occurrences (taxon_class);
CREATE INDEX IF NOT EXISTS ix_occurrences_region ON occurrences (region);
CREATE INDEX IF NOT EXISTS ix_occurrences_date   ON occurrences (event_date);

-- audit log: one row per execution of the loading stage
CREATE TABLE IF NOT EXISTS load_runs (
    run_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at     TEXT    NOT NULL,
    source_file    TEXT    NOT NULL,
    rows_in_file   INTEGER NOT NULL,
    inserted       INTEGER NOT NULL,
    updated        INTEGER NOT NULL,
    unchanged      INTEGER NOT NULL,
    rows_in_table  INTEGER NOT NULL
);
