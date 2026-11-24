"""Pydantic models = the message contract between Producer and Consumer.

* OccurrenceRecord - one GBIF occurrence, flattened to the fields we use.
  It can be built directly from the raw GBIF JSON (camelCase aliases such as
  `gbifID`, `decimalLatitude`) and is serialized with snake_case names.
* KafkaEnvelope   - the JSON value of every Kafka message (metadata + record).

Pydantic checks *structure and types* only (is gbif_id an integer, is latitude
a number, ...). Business rules (coordinate ranges, allowed categories, date
logic) are deliberately left to the Pandera schema in validator.py, which
validates the whole dataset after it has been received.
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator


def _alias(gbif_name: str, our_name: str) -> AliasChoices:
    """Accept both the GBIF camelCase name and our snake_case name."""
    return AliasChoices(gbif_name, our_name)


class OccurrenceRecord(BaseModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    gbif_id: int = Field(validation_alias=_alias("gbifID", "gbif_id"))
    scientific_name: str | None = Field(None, validation_alias=_alias("scientificName", "scientific_name"))
    species: str | None = None
    kingdom: str | None = None
    phylum: str | None = None
    taxon_class: str | None = Field(None, validation_alias=_alias("class", "taxon_class"))
    taxon_order: str | None = Field(None, validation_alias=_alias("order", "taxon_order"))
    family: str | None = None
    genus: str | None = None
    taxon_rank: str | None = Field(None, validation_alias=_alias("taxonRank", "taxon_rank"))
    basis_of_record: str | None = Field(None, validation_alias=_alias("basisOfRecord", "basis_of_record"))
    occurrence_status: str | None = Field(None, validation_alias=_alias("occurrenceStatus", "occurrence_status"))
    iucn_category: str | None = Field(None, validation_alias=_alias("iucnRedListCategory", "iucn_category"))
    state_province: str | None = Field(None, validation_alias=_alias("stateProvince", "state_province"))
    latitude: float | None = Field(None, validation_alias=_alias("decimalLatitude", "latitude"))
    longitude: float | None = Field(None, validation_alias=_alias("decimalLongitude", "longitude"))
    coord_uncertainty_m: float | None = Field(
        None, validation_alias=_alias("coordinateUncertaintyInMeters", "coord_uncertainty_m"))
    individual_count: int | None = Field(None, validation_alias=_alias("individualCount", "individual_count"))
    event_date: str | None = Field(None, validation_alias=_alias("eventDate", "event_date"))  # may be a range
    year: int | None = None
    month: int | None = None
    day: int | None = None
    dataset_name: str | None = Field(None, validation_alias=_alias("datasetName", "dataset_name"))
    institution_code: str | None = Field(None, validation_alias=_alias("institutionCode", "institution_code"))
    license: str | None = None
    last_interpreted: str | None = Field(None, validation_alias=_alias("lastInterpreted", "last_interpreted"))
    issue_count: int = Field(0, ge=0)
    media_count: int = Field(0, ge=0)

    @model_validator(mode="before")
    @classmethod
    def _derive_counts(cls, data: Any) -> Any:
        """Raw GBIF JSON has `issues` / `media` lists -> turn them into counts."""
        if isinstance(data, dict) and ("issues" in data or "media" in data):
            data = dict(data)
            data.setdefault("issue_count", len(data.get("issues") or []))
            data.setdefault("media_count", len(data.get("media") or []))
        return data


class KafkaEnvelope(BaseModel):
    """JSON value of a Kafka message."""
    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(min_length=1)
    seq: int = Field(ge=1)
    source: Literal["gbif-occurrence-api"] = "gbif-occurrence-api"
    produced_at: datetime
    record: OccurrenceRecord
