# SIS2 — Biodiversity of Kazakhstan: a Kafka + Pandera data pipeline

**Course:** Data Collection & Preparation
**Data source:** [GBIF Occurrence API](https://techdocs.gbif.org/en/openapi/v1/occurrence) — biodiversity observations in Kazakhstan (`country=KZ`), see <https://www.gbif.org/occurrence/search?country=KZ>

The pipeline collects ~1 050 real occurrence records (birds, plants, insects, …) from 2015–2025, streams them through a real
Apache Kafka broker, checks them with Pandera, cleans them with pandas and NumPy, stores them idempotently in SQLite3, and runs
an exploratory analysis with six figures.

```
GBIF REST API ──► api_client.py ──► data/raw/*.jsonl  (raw data kept untouched)
                                          │
                                          ▼
                        producer.py  (FastStream KafkaBroker, Pydantic → JSON)
                                          │
                              Kafka topic "gbif-kz-occurrences" (3 partitions)
                                          │
                        consumer.py  (FastStream subscriber, JSON → Pydantic)
                                          ▼
                 validator.py (Pandera) ──► invalid_records.csv (quarantine)
                                          ▼
                 cleaner.py (pandas + NumPy) ──► summaries/*.csv
                                          ▼
                 loader.py (SQLite3 UPSERT, run twice) ──► data/output.db
                                          ▼
                 analysis.py (EDA, Matplotlib/Seaborn) ──► figures/*.png
                                          ▼
                 main.py — stage 7: count comparison + PASS/FAIL checks
```

## 1. Repository structure

```
sis2/
├── README.md               this file
├── pyproject.toml          project metadata + dependencies (managed with uv)
├── uv.lock                 exact pinned versions of all packages
├── docker-compose.yml      single-node Apache Kafka 3.9 broker (KRaft mode, no ZooKeeper)
├── .env.example            all configurable settings (no secrets are needed)
├── main.py                 runs the whole pipeline
├── src/
│   ├── config.py           pydantic-settings configuration (env / .env / defaults)
│   ├── models.py           Pydantic models = Kafka message contract
│   ├── api_client.py       stage 1  GBIF API collection
│   ├── kafka_admin.py      topic create / reset / describe
│   ├── producer.py         stage 2a Kafka Producer
│   ├── consumer.py         stage 2b Kafka Consumer
│   ├── validator.py        stage 3  Pandera schema + test records
│   ├── cleaner.py          stage 4  cleaning & transformation
│   ├── loader.py           stage 5  SQLite3 loading + idempotency test
│   ├── analysis.py         stage 6  EDA and figures
│   └── stats.py            per-stage record counts (data/pipeline_stats.json)
├── sql/
│   ├── schema.sql          table definitions
│   └── verification_queries.sql
├── data/                   raw, intermediate and final data + output.db
├── figures/                generated PNG figures
├── docs/
│   ├── data_dictionary.md
│   └── pipeline_run_log.txt  console output of the reported run
└── report/
    ├── project_report.pdf
    ├── project_report.md   source of the PDF
    └── build_pdf.sh        rebuilds the PDF (pandoc + Google Chrome)
```

## 2. Requirements

| Software | Version used |
|---|---|
| Python | ≥ 3.12 (developed on 3.14) |
| Docker + Docker Compose | any recent version (for the Kafka broker) |
| Apache Kafka | 3.9.1 (`apache/kafka` image) |
| Python libraries | requests, pandas 3, NumPy 2, Pandera 0.36, Pydantic 2, pydantic-settings, FastStream 0.7 (aiokafka), Matplotlib, Seaborn |

Internet access to `api.gbif.org` is required for stage 1 (no API key needed). Without internet, run with `--skip-collect`
to reuse the raw data committed in `data/raw/`.

## 3. Installation

```bash
git clone <repo-url> && cd sis2

# install uv once: https://docs.astral.sh/uv/getting-started/installation/
curl -LsSf https://astral.sh/uv/install.sh | sh

uv sync                              # creates .venv with the exact versions from uv.lock
source .venv/bin/activate            # Windows: .venv\Scripts\activate
```

All commands below assume the activated environment; alternatively prefix them with `uv run`,
e.g. `uv run python main.py`.

## 4. Configuration

Everything has a working default, so no configuration is needed. To change something, copy `.env.example` to `.env`
(or export environment variables) — `src/config.py` reads them with pydantic-settings and validates the types.

| Variable | Default | Meaning |
|---|---|---|
| `GBIF_COUNTRY` | `KZ` | ISO country code |
| `GBIF_YEAR_FROM` / `GBIF_YEAR_TO` | `2015` / `2025` | period |
| `GBIF_RECORDS_PER_MONTH` | `8` | records per (year, month) stratum → 11 × 12 × 8 ≈ 1 056 |
| `GBIF_RANDOM_SEED` | `42` | makes the random offsets reproducible |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | broker address |
| `KAFKA_TOPIC` | `gbif-kz-occurrences` | topic name |
| `KAFKA_TOPIC_PARTITIONS` | `3` | partitions when the topic is created |
| `KAFKA_CONSUMER_GROUP` | `sis2-gbif-consumer` | consumer group |
| `KAFKA_CONSUMER_IDLE_TIMEOUT_S` | `10` | consumer stops after this many idle seconds |
| `SQLITE_DB_PATH` | `data/output.db` | database file |

`.env` is git-ignored. The GBIF API needs no credentials, so nothing secret is stored anywhere.

## 5. Start the Kafka broker

```bash
docker compose up -d                 # starts container "sis2-kafka" on localhost:9092
docker compose ps                    # wait until STATUS shows "(healthy)"
```

Topic configuration: the broker has `auto.create.topics.enable=false`, so the topic is created explicitly by the code
(3 partitions, replication factor 1, retention 7 days). You can create / inspect / reset it manually:

```bash
python -m src.kafka_admin create     # create topic if missing
python -m src.kafka_admin describe   # partitions, end offsets, committed offsets, lag
python -m src.kafka_admin reset      # delete + recreate (empty topic)

# the same with Kafka's own CLI inside the container
docker exec sis2-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic gbif-kz-occurrences
```

## 6. Run the complete pipeline

```bash
python main.py                   # all 7 stages, ~20–60 s
python main.py --skip-collect    # reuse data/raw/ (no API calls)
```

`main.py` resets the topic first, so every run is one clean finite stream. At the end it prints the verification table
and `Pipeline finished ... - verification PASS`.

## 7. Run the stages one by one (execution order)

```bash
python -m src.api_client                 # 1. collect  -> data/raw/gbif_raw_records.jsonl, data/raw_data.csv
python -m src.producer                   # 2a. publish all records to Kafka
python -m src.consumer                   # 2b. consume until 10 s idle -> data/consumed_records.csv
python -m src.validator                  # 3. Pandera  -> validated / invalid / validation_errors csv
python -m src.cleaner                    # 4. clean    -> data/processed_records.csv, data/summaries/
python -m src.loader --twice             # 5. SQLite: load twice + idempotency test + SQL verification
python -m src.analysis                   # 6. EDA      -> figures/*.png, data/summaries/eda_summary.txt
```

Producer and consumer can also run at the same time in two terminals (start the consumer first, then the producer).

Useful options:

```bash
python -m src.producer --limit 200           # publish only 200 records
python -m src.producer --inject-invalid 3    # also send 3 broken messages -> consumer rejects them
python -m src.consumer --max-messages 100    # stop after 100 messages
python -m src.consumer --from-beginning      # new consumer group -> re-read the whole topic
python -m src.validator --no-test-records    # validate only the real data
```

Because offsets are committed after the data is saved, running `python -m src.consumer` a second time reads **0** new
messages — that is the expected Kafka consumer-group behaviour.

## 8. Database generation and verification

`python -m src.loader --twice` creates `data/output.db` from `sql/schema.sql`, loads the data twice and runs every query
from `sql/verification_queries.sql`. Run your own queries with:

```bash
sqlite3 data/output.db "SELECT COUNT(*) FROM occurrences;"
sqlite3 data/output.db "SELECT * FROM load_runs;"
sqlite3 data/output.db < sql/verification_queries.sql
```

(or with Python: `python -c "import sqlite3; print(sqlite3.connect('data/output.db').execute('select count(*) from occurrences').fetchone())"`)

## 9. Expected output

| Stage | Expected count (reported run, 2026-11-24) |
|---|---|
| Records collected from GBIF | 1 053 |
| Messages published / consumed | 1 053 / 1 053 (+3 broken demo messages, all rejected) |
| Pandera valid / invalid real / invalid test | 1 052 / 1 / 3 |
| Records after cleaning | 1 031 (21 duplicate observations removed) |
| Records in SQLite | 1 031 — second load: 0 inserted, 0 updated, 1 031 unchanged |
| Figures | 6 PNG files in `figures/` |

GBIF is a live database, so counts can differ slightly if it changes, but all PASS checks must still hold.

## 10. How to check that everything worked

1. `main.py` ends with `verification PASS`; all 12 lines in `data/summaries/pipeline_verification.txt` are `[PASS]`.
2. `python -m src.kafka_admin describe` → `messages_in_topic` equals the number published (+3 demo) and `lag` is 0.
3. `sqlite3 data/output.db "SELECT * FROM load_runs"` → the second run shows `inserted=0, updated=0`.
4. `data/invalid_records.csv` contains the 3 test rows (`gbif_id` −1, −2, −3) and none of them is in the database.
5. `figures/` contains `fig1` … `fig6`.

## 11. Stop

```bash
docker compose down          # stop the broker (add -v to also delete Kafka data)
```
