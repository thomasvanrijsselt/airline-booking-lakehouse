import pytest

from airline_booking_lakehouse.quality import (
    DataQualityError,
    check_foreign_key,
    check_non_negative,
    check_not_empty,
    check_not_null,
    check_unique,
    validate_gold_tables,
)


def test_check_not_empty_accepts_dataframe_with_rows(spark):
    df = spark.createDataFrame([(1,)], ["id"])

    check_not_empty(df, "example_table")


def test_check_not_empty_rejects_empty_dataframe(spark):
    df = spark.createDataFrame([], "id long")

    with pytest.raises(
        DataQualityError,
        match="example_table must not be empty",
    ):
        check_not_empty(df, "example_table")


def test_check_not_null_accepts_complete_rows(spark):
    df = spark.createDataFrame(
        [
            (1, "AMS"),
            (2, "LHR"),
        ],
        ["id", "airport"],
    )

    check_not_null(
        df=df,
        columns=["id", "airport"],
        table_name="example_table",
    )


def test_check_not_null_rejects_null_in_required_column(spark):
    df = spark.createDataFrame(
        [
            (1, "AMS"),
            (2, None),
        ],
        "id long, airport string",
    )

    with pytest.raises(
        DataQualityError,
        match="example_table contains 1 rows with null values",
    ):
        check_not_null(
            df=df,
            columns=["id", "airport"],
            table_name="example_table",
        )


def test_check_unique_accepts_unique_keys(spark):
    df = spark.createDataFrame(
        [
            ("B001", 100.0),
            ("B002", 150.0),
        ],
        ["booking_id", "amount"],
    )

    check_unique(
        df=df,
        columns=["booking_id"],
        table_name="fact_booking",
    )


def test_check_unique_rejects_duplicate_keys(spark):
    df = spark.createDataFrame(
        [
            ("B001", 100.0),
            ("B001", 150.0),
        ],
        ["booking_id", "amount"],
    )

    with pytest.raises(
        DataQualityError,
        match="fact_booking contains 1 duplicate keys",
    ):
        check_unique(
            df=df,
            columns=["booking_id"],
            table_name="fact_booking",
        )


def test_check_foreign_key_accepts_existing_dimension_keys(spark):
    fact_df = spark.createDataFrame(
        [
            ("B001", "F001"),
            ("B002", "F002"),
        ],
        ["booking_id", "flight_key"],
    )

    dimension_df = spark.createDataFrame(
        [
            ("F001",),
            ("F002",),
        ],
        ["flight_key"],
    )

    check_foreign_key(
        fact_df=fact_df,
        dimension_df=dimension_df,
        fact_columns=["flight_key"],
        dimension_columns=["flight_key"],
        fact_table_name="fact_booking",
        dimension_table_name="dim_flight",
    )


def test_check_foreign_key_rejects_missing_dimension_key(spark):
    fact_df = spark.createDataFrame(
        [
            ("B001", "F001"),
            ("B002", "F999"),
        ],
        ["booking_id", "flight_key"],
    )

    dimension_df = spark.createDataFrame(
        [("F001",)],
        ["flight_key"],
    )

    with pytest.raises(
        DataQualityError,
        match=("fact_booking contains 1 foreign keys that do not exist in dim_flight"),
    ):
        check_foreign_key(
            fact_df=fact_df,
            dimension_df=dimension_df,
            fact_columns=["flight_key"],
            dimension_columns=["flight_key"],
            fact_table_name="fact_booking",
            dimension_table_name="dim_flight",
        )


def test_check_non_negative_accepts_zero_and_positive_values(spark):
    df = spark.createDataFrame(
        [
            ("B001", 0.0),
            ("B002", 150.0),
        ],
        ["booking_id", "amount"],
    )

    check_non_negative(
        df=df,
        columns=["amount"],
        table_name="fact_booking",
    )


def test_check_non_negative_rejects_negative_values(spark):
    df = spark.createDataFrame(
        [
            ("B001", 100.0),
            ("B002", -20.0),
        ],
        ["booking_id", "amount"],
    )

    with pytest.raises(
        DataQualityError,
        match="fact_booking contains 1 rows with negative values",
    ):
        check_non_negative(
            df=df,
            columns=["amount"],
            table_name="fact_booking",
        )


def test_validate_gold_tables_accepts_valid_model(spark):
    dim_date = spark.createDataFrame(
        [
            (
                20260825,
                "2026-08-25",
                2026,
                3,
                8,
                "August",
                25,
                3,
            ),
        ],
        [
            "date_key",
            "departure_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day_of_month",
            "day_of_week",
        ],
    )

    dim_flight = spark.createDataFrame(
        [
            (
                "flight-key-001",
                "KL1001",
                "AMS",
                "LHR",
                "AMS-LHR",
            ),
        ],
        [
            "flight_key",
            "flight_number",
            "departure_airport",
            "arrival_airport",
            "route",
        ],
    )

    fact_booking = spark.createDataFrame(
        [
            (
                "B001",
                "E001",
                20260825,
                "flight-key-001",
                "ACTIVE",
                2,
                250.0,
                "EUR",
                "WEB",
            ),
        ],
        [
            "booking_id",
            "current_event_id",
            "date_key",
            "flight_key",
            "booking_status",
            "passenger_count",
            "amount",
            "currency",
            "booking_channel",
        ],
    )

    validate_gold_tables(
        dim_date=dim_date,
        dim_flight=dim_flight,
        fact_booking=fact_booking,
    )
