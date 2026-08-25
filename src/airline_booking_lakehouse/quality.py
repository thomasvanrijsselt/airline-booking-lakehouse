import argparse
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class DataQualityError(Exception):
    """Raised when a table-level data quality check fails."""


def check_not_empty(df: DataFrame, table_name: str) -> None:
    if df.limit(1).count() == 0:
        raise DataQualityError(f"{table_name} must not be empty")


def check_not_null(
    df: DataFrame,
    columns: Sequence[str],
    table_name: str,
) -> None:
    condition = F.lit(False)

    for column in columns:
        condition = condition | F.col(column).isNull()

    invalid_count = df.filter(condition).count()

    if invalid_count > 0:
        raise DataQualityError(
            f"{table_name} contains {invalid_count} rows with null values "
            f"in required columns: {', '.join(columns)}"
        )


def check_unique(
    df: DataFrame,
    columns: Sequence[str],
    table_name: str,
) -> None:
    duplicate_count = df.groupBy(*columns).count().filter(F.col("count") > 1).count()

    if duplicate_count > 0:
        raise DataQualityError(
            f"{table_name} contains {duplicate_count} duplicate keys "
            f"for columns: {', '.join(columns)}"
        )


def check_non_negative(
    df: DataFrame,
    columns: Sequence[str],
    table_name: str,
) -> None:
    condition = F.lit(False)

    for column in columns:
        condition = condition | (F.col(column) < 0)

    invalid_count = df.filter(condition).count()

    if invalid_count > 0:
        raise DataQualityError(
            f"{table_name} contains {invalid_count} rows with negative values "
            f"in columns: {', '.join(columns)}"
        )


def check_foreign_key(
    fact_df: DataFrame,
    dimension_df: DataFrame,
    fact_columns: Sequence[str],
    dimension_columns: Sequence[str],
    fact_table_name: str,
    dimension_table_name: str,
) -> None:
    if len(fact_columns) != len(dimension_columns):
        raise ValueError("fact_columns and dimension_columns must have the same length")

    fact_keys = fact_df.select(*fact_columns).dropDuplicates()

    dimension_keys = dimension_df.select(
        *[
            F.col(dimension_column).alias(fact_column)
            for fact_column, dimension_column in zip(
                fact_columns,
                dimension_columns,
                strict=True,
            )
        ]
    ).dropDuplicates()

    missing_key_count = fact_keys.join(
        dimension_keys,
        on=list(fact_columns),
        how="left_anti",
    ).count()

    if missing_key_count > 0:
        raise DataQualityError(
            f"{fact_table_name} contains {missing_key_count} foreign keys "
            f"that do not exist in {dimension_table_name}: "
            f"{', '.join(fact_columns)}"
        )


def validate_gold_tables(
    dim_date: DataFrame,
    dim_flight: DataFrame,
    fact_booking: DataFrame,
) -> None:
    check_not_empty(dim_date, "dim_date")
    check_not_empty(dim_flight, "dim_flight")
    check_not_empty(fact_booking, "fact_booking")

    check_not_null(
        dim_date,
        ["date_key", "departure_date"],
        "dim_date",
    )
    check_not_null(
        dim_flight,
        [
            "flight_key",
            "flight_number",
            "departure_airport",
            "arrival_airport",
            "route",
        ],
        "dim_flight",
    )
    check_not_null(
        fact_booking,
        [
            "booking_id",
            "current_event_id",
            "date_key",
            "flight_key",
            "booking_status",
            "passenger_count",
            "amount",
            "currency",
        ],
        "fact_booking",
    )

    check_unique(dim_date, ["date_key"], "dim_date")
    check_unique(dim_date, ["departure_date"], "dim_date")

    check_unique(dim_flight, ["flight_key"], "dim_flight")
    check_unique(
        dim_flight,
        [
            "flight_number",
            "departure_airport",
            "arrival_airport",
        ],
        "dim_flight",
    )

    check_unique(fact_booking, ["booking_id"], "fact_booking")

    check_foreign_key(
        fact_df=fact_booking,
        dimension_df=dim_date,
        fact_columns=["date_key"],
        dimension_columns=["date_key"],
        fact_table_name="fact_booking",
        dimension_table_name="dim_date",
    )
    check_foreign_key(
        fact_df=fact_booking,
        dimension_df=dim_flight,
        fact_columns=["flight_key"],
        dimension_columns=["flight_key"],
        fact_table_name="fact_booking",
        dimension_table_name="dim_flight",
    )

    check_non_negative(
        fact_booking,
        ["passenger_count", "amount"],
        "fact_booking",
    )


def run_quality_checks(
    spark: SparkSession,
    dim_date_table: str,
    dim_flight_table: str,
    fact_booking_table: str,
) -> None:
    dim_date = spark.table(dim_date_table)
    dim_flight = spark.table(dim_flight_table)
    fact_booking = spark.table(fact_booking_table)

    validate_gold_tables(
        dim_date=dim_date,
        dim_flight=dim_flight,
        fact_booking=fact_booking,
    )

    print("All Gold table quality checks passed")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Gold table quality rules.")
    parser.add_argument("--dim-date-table", required=True)
    parser.add_argument("--dim-flight-table", required=True)
    parser.add_argument("--fact-booking-table", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    spark = SparkSession.builder.getOrCreate()

    run_quality_checks(
        spark=spark,
        dim_date_table=args.dim_date_table,
        dim_flight_table=args.dim_flight_table,
        fact_booking_table=args.fact_booking_table,
    )


if __name__ == "__main__":
    main()
