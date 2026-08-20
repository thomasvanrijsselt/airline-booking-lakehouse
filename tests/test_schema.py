from pyspark.sql.types import DecimalType, TimestampType

from airline_booking_lakehouse.schemas import BOOKING_EVENT_SCHEMA


def test_booking_event_schema_contains_expected_fields() -> None:
    assert BOOKING_EVENT_SCHEMA.fieldNames() == [
        "event_id",
        "event_type",
        "event_timestamp",
        "booking_id",
        "flight_number",
        "departure_airport",
        "arrival_airport",
        "departure_date",
        "passenger_count",
        "amount",
        "currency",
        "booking_channel",
    ]


def test_event_timestamp_has_timestamp_type() -> None:
    assert isinstance(
        BOOKING_EVENT_SCHEMA["event_timestamp"].dataType,
        TimestampType,
    )


def test_amount_uses_fixed_decimal_type() -> None:
    assert BOOKING_EVENT_SCHEMA["amount"].dataType == DecimalType(10, 2)


def test_source_fields_are_nullable() -> None:
    assert all(field.nullable for field in BOOKING_EVENT_SCHEMA)

