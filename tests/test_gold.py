from datetime import date, datetime

from pyspark.sql import SparkSession

from airline_booking_lakehouse.gold import (
    build_dim_date,
    build_dim_flight,
    build_fact_booking,
    derive_current_booking_state,
)


def test_derive_current_booking_state_keeps_latest_event(
    spark: SparkSession,
) -> None:
    events = spark.createDataFrame(
        [
            (
                "evt-001",
                "booking-001",
                "BOOKING_CREATED",
                datetime(2026, 8, 1, 10, 0),
                datetime(2026, 8, 1, 10, 5),
            ),
            (
                "evt-002",
                "booking-001",
                "BOOKING_UPDATED",
                datetime(2026, 8, 2, 10, 0),
                datetime(2026, 8, 2, 10, 5),
            ),
            (
                "evt-003",
                "booking-001",
                "BOOKING_CANCELLED",
                datetime(2026, 8, 3, 10, 0),
                datetime(2026, 8, 3, 10, 5),
            ),
            (
                "evt-004",
                "booking-002",
                "BOOKING_CREATED",
                datetime(2026, 8, 2, 12, 0),
                datetime(2026, 8, 2, 12, 5),
            ),
        ],
        schema="""
            event_id string,
            booking_id string,
            event_type string,
            event_timestamp timestamp,
            _ingested_at timestamp
        """,
    )

    result = derive_current_booking_state(events)

    bookings = {row["booking_id"]: row for row in result.collect()}

    assert len(bookings) == 2

    assert bookings["booking-001"]["event_id"] == "evt-003"
    assert bookings["booking-001"]["booking_status"] == "CANCELLED"

    assert bookings["booking-002"]["event_id"] == "evt-004"
    assert bookings["booking-002"]["booking_status"] == "ACTIVE"


def test_derive_current_booking_state_uses_event_time_for_late_events(
    spark: SparkSession,
) -> None:
    events = spark.createDataFrame(
        [
            (
                "evt-newer-business-event",
                "booking-001",
                "BOOKING_UPDATED",
                datetime(2026, 8, 3, 10, 0),
                datetime(2026, 8, 3, 10, 5),
            ),
            (
                "evt-late-arrival",
                "booking-001",
                "BOOKING_CREATED",
                datetime(2026, 8, 1, 10, 0),
                datetime(2026, 8, 4, 10, 5),
            ),
        ],
        schema="""
            event_id string,
            booking_id string,
            event_type string,
            event_timestamp timestamp,
            _ingested_at timestamp
        """,
    )

    result = derive_current_booking_state(events).collect()

    assert len(result) == 1
    assert result[0]["event_id"] == "evt-newer-business-event"
    assert result[0]["booking_status"] == "ACTIVE"


def test_build_dim_date_creates_one_row_per_departure_date(
    spark: SparkSession,
) -> None:
    bookings = spark.createDataFrame(
        [
            ("booking-001", date(2026, 9, 14)),
            ("booking-002", date(2026, 9, 14)),
            ("booking-003", date(2026, 10, 2)),
        ],
        schema="""
            booking_id string,
            departure_date date
        """,
    )

    result = {row["date_key"]: row for row in build_dim_date(bookings).collect()}

    assert len(result) == 2

    september = result[20260914]

    assert september["departure_date"] == date(2026, 9, 14)
    assert september["year"] == 2026
    assert september["quarter"] == 3
    assert september["month"] == 9
    assert september["day_of_month"] == 14


def test_build_dim_flight_creates_one_row_per_flight_and_route(
    spark: SparkSession,
) -> None:
    bookings = spark.createDataFrame(
        [
            ("booking-001", "KL1001", "AMS", "LHR"),
            ("booking-002", "KL1001", "AMS", "LHR"),
            ("booking-003", "KL1002", "AMS", "CDG"),
        ],
        schema="""
            booking_id string,
            flight_number string,
            departure_airport string,
            arrival_airport string
        """,
    )

    result = build_dim_flight(bookings).collect()

    assert len(result) == 2
    assert all(row["flight_key"] is not None for row in result)

    routes = {row["route"] for row in result}

    assert routes == {"AMS-LHR", "AMS-CDG"}


def test_build_fact_booking_creates_one_row_per_current_booking(
    spark: SparkSession,
) -> None:
    current_bookings = spark.createDataFrame(
        [
            (
                "booking-001",
                "evt-002",
                datetime(2026, 8, 2, 10, 0),
                date(2026, 9, 14),
                "KL1001",
                "AMS",
                "LHR",
                "ACTIVE",
                2,
                450.00,
                "EUR",
                "web",
            ),
            (
                "booking-002",
                "evt-004",
                datetime(2026, 8, 3, 10, 0),
                date(2026, 10, 2),
                "KL1002",
                "AMS",
                "CDG",
                "CANCELLED",
                1,
                180.00,
                "EUR",
                "app",
            ),
        ],
        schema="""
            booking_id string,
            event_id string,
            event_timestamp timestamp,
            departure_date date,
            flight_number string,
            departure_airport string,
            arrival_airport string,
            booking_status string,
            passenger_count integer,
            amount double,
            currency string,
            booking_channel string
        """,
    )

    dim_date = build_dim_date(current_bookings)
    dim_flight = build_dim_flight(current_bookings)

    result = build_fact_booking(
        current_bookings=current_bookings,
        dim_date=dim_date,
        dim_flight=dim_flight,
    ).collect()

    facts = {row["booking_id"]: row for row in result}

    assert len(facts) == 2

    assert facts["booking-001"]["date_key"] == 20260914
    assert facts["booking-001"]["flight_key"] is not None
    assert facts["booking-001"]["passenger_count"] == 2
    assert facts["booking-001"]["amount"] == 450.00
    assert facts["booking-001"]["booking_status"] == "ACTIVE"

    assert facts["booking-002"]["booking_status"] == "CANCELLED"
