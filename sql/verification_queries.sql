-- name: total_rows
SELECT COUNT(*) AS total_rows FROM occurrences;

-- name: duplicate_primary_keys (must be 0 rows)
SELECT gbif_id, COUNT(*) AS n FROM occurrences GROUP BY gbif_id HAVING n > 1;

-- name: duplicate_observations (must be 0 rows)
SELECT scientific_name, latitude, longitude, event_date, COUNT(*) AS n
FROM occurrences GROUP BY 1, 2, 3, 4 HAVING n > 1;

-- name: null_check_key_columns (must be all 0)
SELECT SUM(scientific_name IS NULL) AS null_names, SUM(latitude IS NULL) AS null_lat,
       SUM(event_date IS NULL) AS null_dates, SUM(region IS NULL) AS null_regions
FROM occurrences;

-- name: rows_by_kingdom
SELECT kingdom, COUNT(*) AS records, COUNT(DISTINCT species) AS species
FROM occurrences GROUP BY kingdom ORDER BY records DESC;

-- name: rows_by_year
SELECT year, COUNT(*) AS records FROM occurrences GROUP BY year ORDER BY year;

-- name: top_regions
SELECT region, COUNT(*) AS records, COUNT(DISTINCT species) AS species,
       ROUND(AVG(coord_uncertainty_m), 1) AS avg_uncertainty_m
FROM occurrences GROUP BY region ORDER BY records DESC LIMIT 10;

-- name: threatened_species
SELECT species, iucn_category, COUNT(*) AS records, GROUP_CONCAT(DISTINCT region) AS regions
FROM occurrences WHERE is_threatened = 1
GROUP BY species, iucn_category ORDER BY records DESC;

-- name: date_and_coordinate_range
SELECT MIN(event_date) AS first_date, MAX(event_date) AS last_date,
       MIN(latitude) AS min_lat, MAX(latitude) AS max_lat,
       MIN(longitude) AS min_lon, MAX(longitude) AS max_lon
FROM occurrences;

-- name: load_history
SELECT run_id, started_at, rows_in_file, inserted, updated, unchanged, rows_in_table
FROM load_runs ORDER BY run_id;
