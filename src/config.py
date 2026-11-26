"""Central configuration built with pydantic-settings.

Values are read (in priority order) from: environment variables -> `.env`
file in the project root -> defaults below. Types are validated by Pydantic,
so e.g. KAFKA_TOPIC_PARTITIONS=abc fails immediately with a clear error.
Nothing personal or secret is hard-coded; see .env.example.
"""

from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=PROJECT_ROOT / ".env", env_file_encoding="utf-8",
                                      extra="ignore")

    # --- GBIF Occurrence API ---------------------------------------------
    gbif_base_url: str = "https://api.gbif.org/v1/occurrence/search"
    gbif_country: str = Field("KZ", min_length=2, max_length=2)
    gbif_year_from: int = Field(2015, ge=1900)
    gbif_year_to: int = Field(2025, ge=1900)
    # stratified sampling: one page of `records_per_month` records for every
    # (year, month), starting at a random offset < gbif_max_random_offset
    gbif_records_per_month: int = Field(8, gt=0, le=300)
    gbif_max_random_offset: int = Field(3000, ge=0, le=10000)  # deep offsets are very slow on GBIF
    gbif_random_seed: int = 42
    gbif_workers: int = Field(4, ge=1, le=8)  # max concurrent HTTP requests (asyncio semaphore)
    gbif_timeout_s: float = Field(30, gt=0)
    gbif_max_retries: int = Field(5, ge=1)  # back-off 2, 4, 8, 16 s

    # --- Kafka -----------------------------------------------------------
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "gbif-kz-occurrences"
    kafka_topic_partitions: int = Field(3, ge=1)
    kafka_topic_replication: int = Field(1, ge=1)
    kafka_consumer_group: str = "sis2-gbif-consumer"
    # consumer stops after this many seconds without new messages
    kafka_consumer_idle_timeout_s: float = Field(10, gt=0)

    # --- Storage ---------------------------------------------------------
    data_dir: Path = PROJECT_ROOT / "data"
    figures_dir: Path = PROJECT_ROOT / "figures"
    sqlite_db_path: Path = PROJECT_ROOT / "data" / "output.db"

    @model_validator(mode="after")
    def _check(self) -> "Settings":
        if self.gbif_year_from > self.gbif_year_to:
            raise ValueError("GBIF_YEAR_FROM must be <= GBIF_YEAR_TO")
        # relative paths from .env are resolved against the project root
        for name in ("data_dir", "figures_dir", "sqlite_db_path"):
            p = getattr(self, name)
            if not p.is_absolute():
                setattr(self, name, PROJECT_ROOT / p)
        return self

    # --- derived file locations -------------------------------------------
    @property
    def raw_dir(self) -> Path: return self.data_dir / "raw"
    @property
    def raw_jsonl(self) -> Path: return self.raw_dir / "gbif_raw_records.jsonl"
    @property
    def year_facets_json(self) -> Path: return self.raw_dir / "gbif_year_counts.json"
    @property
    def raw_csv(self) -> Path: return self.data_dir / "raw_data.csv"
    @property
    def consumed_csv(self) -> Path: return self.data_dir / "consumed_records.csv"
    @property
    def valid_csv(self) -> Path: return self.data_dir / "validated_records.csv"
    @property
    def invalid_csv(self) -> Path: return self.data_dir / "invalid_records.csv"
    @property
    def validation_errors_csv(self) -> Path: return self.data_dir / "validation_errors.csv"
    @property
    def processed_csv(self) -> Path: return self.data_dir / "processed_records.csv"
    @property
    def summary_dir(self) -> Path: return self.data_dir / "summaries"
    @property
    def stats_json(self) -> Path: return self.data_dir / "pipeline_stats.json"


settings = Settings()
