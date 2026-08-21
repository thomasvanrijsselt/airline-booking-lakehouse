from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from airline_booking_lakehouse.schemas import BOOKING_EVENT_SCHEMA
from airline_booking_lakehouse.silver import (
    add_quarantine_id,
    apply_quality_rules,
    deduplicate_events,
    normalize_events,
    split_by_quality,
    upsert_delta_table,
)


def booking_event(**overrides: object) -> dict[str, object]:
    event: dict[str, object] = {
        "event_id": "evt-001",
        "event_type": "BOOKING_CREATED",
        "event_timestamp": datetime(2026, 8, 1, 9, 0),
        "booking_id": "BKG-001",
        "flight_number": "AL100",
        "departure_airport": "AMS",
        "arrival_airport": "LHR",
        "departure_date": date(2026, 9, 1),
        "passenger_count": 1,
        "amount": Decimal("120.50"),
        "currency": "EUR",
        "booking_channel": "web",
    }
    event.update(overrides)
    return event


def test_normalize_events_standardizes_string_fields(
    spark: SparkSession,
) -> None:
    events = spark.createDataFrame(
        [
            booking_event(
                event_id=" evt-001 ",
                event_type=" booking_created ",
                booking_id=" BKG-001 ",
                flight_number=" al100 ",
                departure_airport=" ams ",
                arrival_airport=" lhr ",
                currency=" eur ",
                booking_channel=" WEB ",
            )
        ],
        schema=BOOKING_EVENT_SCHEMA,
    )

    result = normalize_events(events).first()

    assert result.event_id == "evt-001"
    assert result.event_type == "BOOKING_CREATED"
    assert result.booking_id == "BKG-001"
    assert result.flight_number == "AL100"
    assert result.departure_airport == "AMS"
    assert result.arrival_airport == "LHR"
    assert result.currency == "EUR"
    assert result.booking_channel == "web"


def test_valid_event_passes_all_quality_rules(
    spark: SparkSession,
) -> None:
    events = spark.createDataFrame(
        [booking_event()],
        schema=BOOKING_EVENT_SCHEMA,
    )

    result = apply_quality_rules(normalize_events(events)).first()

    assert result.is_valid is True
    assert result.failed_rules == []


def test_invalid_event_records_all_failed_rules(
    spark: SparkSession,
) -> None:
    events = spark.createDataFrame(
        [
            booking_event(
                event_type="BOOKING_CONFIRMED",
                booking_id=None,
                departure_airport="AMSTERDAM",
                passenger_count=-1,
                amount=Decimal("-50.00"),
            )
        ],
        schema=BOOKING_EVENT_SCHEMA,
    )

    result = apply_quality_rules(normalize_events(events)).first()

    assert result.is_valid is False
    assert set(result.failed_rules) == {
        "booking_id_required",
        "valid_event_type",
        "valid_departure_airport",
        "positive_passenger_count",
        "non_negative_amount",
    }


def test_deduplicate_events_keeps_latest_ingested_copy(
    spark: SparkSession,
) -> None:
    events = spark.createDataFrame(
        [
            (
                "evt-001",
                datetime(2026, 8, 1, 9, 0),
                datetime(2026, 8, 1, 10, 0),
                "batch_001.json",
                Decimal("120.50"),
            ),
            (
                "evt-001",
                datetime(2026, 8, 1, 9, 0),
                datetime(2026, 8, 2, 10, 0),
                "batch_002.json",
                Decimal("130.00"),
            ),
            (
                "evt-002",
                datetime(2026, 8, 1, 9, 15),
                datetime(2026, 8, 1, 10, 0),
                "batch_001.json",
                Decimal("300.00"),
            ),
        ],
        schema="""
            event_id string,
            event_timestamp timestamp,
            _ingested_at timestamp,
            _source_file string,
            amount decimal(10, 2)
        """,
    )

    results = {row.event_id: row for row in deduplicate_events(events).collect()}

    assert len(results) == 2
    assert results["evt-001"].amount == Decimal("130.00")
    assert results["evt-001"]._source_file == "batch_002.json"
    assert results["evt-002"].amount == Decimal("300.00")


def test_sample_data_produces_expected_silver_and_quarantine_counts(
    spark: SparkSession,
) -> None:
    sample_data_path = Path(__file__).parents[1] / "sample_data"

    bronze_events = (
        spark.read.schema(BOOKING_EVENT_SCHEMA)
        .json(str(sample_data_path / "batch_*.json"))
        .withColumn(
            "_ingested_at",
            F.lit(datetime(2026, 8, 4, 12, 0)),
        )
        .withColumn("_source_file", F.lit("sample_data"))
    )

    checked_events = apply_quality_rules(normalize_events(bronze_events))
    valid_events, invalid_events = split_by_quality(checked_events)
    deduplicated_valid_events = deduplicate_events(valid_events)

    invalid_event_ids = {
        row.event_id for row in invalid_events.select("event_id").collect()
    }

    assert bronze_events.count() == 13
    assert valid_events.count() == 10
    assert deduplicated_valid_events.count() == 9
    assert invalid_events.count() == 3
    assert invalid_event_ids == {
        "evt-007",
        "evt-008",
        "evt-012",
    }


def test_quarantine_id_is_stable_and_unique(
    spark: SparkSession,
) -> None:
    events = spark.createDataFrame(
        [
            booking_event(
                event_id="evt-007",
                booking_id=None,
            ),
            booking_event(
                event_id="evt-008",
                departure_airport="AMSTERDAM",
            ),
        ],
        schema=BOOKING_EVENT_SCHEMA,
    ).withColumn("_source_file", F.lit("batch_002.json"))

    first_result = {
        row.event_id: row.quarantine_id for row in add_quarantine_id(events).collect()
    }
    second_result = {
        row.event_id: row.quarantine_id for row in add_quarantine_id(events).collect()
    }

    assert first_result == second_result
    assert len(set(first_result.values())) == 2


def test_upsert_rejects_missing_key_column(
    spark: SparkSession,
) -> None:
    updates = spark.createDataFrame(
        [("evt-001",)],
        schema="event_id string",
    )

    with pytest.raises(
        ValueError,
        match="Merge key 'booking_id' is missing",
    ):
        upsert_delta_table(
            spark=spark,
            updates=updates,
            table_name="unused.table",
            key_column="booking_id",
        )


def test_upsert_rejects_null_key(
    spark: SparkSession,
) -> None:
    updates = spark.createDataFrame(
        [(None, 100)],
        schema="event_id string, amount int",
    )

    with pytest.raises(
        ValueError,
        match="Merge key 'event_id' must not contain null",
    ):
        upsert_delta_table(
            spark=spark,
            updates=updates,
            table_name="unused.table",
            key_column="event_id",
        )
