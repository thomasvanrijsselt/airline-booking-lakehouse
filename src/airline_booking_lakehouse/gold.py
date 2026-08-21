import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from airline_booking_lakehouse.silver import upsert_delta_table


def derive_current_booking_state(events: DataFrame) -> DataFrame:
    latest_event_window = Window.partitionBy("booking_id").orderBy(
        F.col("event_timestamp").desc(),
        F.col("_ingested_at").desc(),
        F.col("event_id").desc(),
    )

    return (
        events.withColumn(
            "_current_state_rank",
            F.row_number().over(latest_event_window),
        )
        .filter(F.col("_current_state_rank") == 1)
        .drop("_current_state_rank")
        .withColumn(
            "booking_status",
            F.when(
                F.col("event_type") == "BOOKING_CANCELLED",
                F.lit("CANCELLED"),
            ).otherwise(F.lit("ACTIVE")),
        )
    )


def build_dim_date(current_bookings: DataFrame) -> DataFrame:
    return (
        current_bookings.select("departure_date")
        .filter(F.col("departure_date").isNotNull())
        .dropDuplicates(["departure_date"])
        .withColumn(
            "date_key",
            F.date_format("departure_date", "yyyyMMdd").cast("int"),
        )
        .withColumn("year", F.year("departure_date"))
        .withColumn("quarter", F.quarter("departure_date"))
        .withColumn("month", F.month("departure_date"))
        .withColumn(
            "month_name",
            F.date_format("departure_date", "MMMM"),
        )
        .withColumn("day_of_month", F.dayofmonth("departure_date"))
        .withColumn(
            "day_of_week",
            F.dayofweek("departure_date"),
        )
        .select(
            "date_key",
            "departure_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day_of_month",
            "day_of_week",
        )
    )


def build_dim_flight(current_bookings: DataFrame) -> DataFrame:
    flight_columns = [
        "flight_number",
        "departure_airport",
        "arrival_airport",
    ]

    return (
        current_bookings.select(*flight_columns)
        .dropDuplicates(flight_columns)
        .withColumn(
            "flight_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("flight_number"),
                    F.col("departure_airport"),
                    F.col("arrival_airport"),
                ),
                256,
            ),
        )
        .withColumn(
            "route",
            F.concat_ws(
                "-",
                F.col("departure_airport"),
                F.col("arrival_airport"),
            ),
        )
        .select(
            "flight_key",
            "flight_number",
            "departure_airport",
            "arrival_airport",
            "route",
        )
    )


def build_fact_booking(
    current_bookings: DataFrame,
    dim_date: DataFrame,
    dim_flight: DataFrame,
) -> DataFrame:
    flight_join_columns = [
        "flight_number",
        "departure_airport",
        "arrival_airport",
    ]

    return (
        current_bookings.alias("booking")
        .join(
            dim_date.select("date_key", "departure_date").alias("date"),
            on="departure_date",
            how="inner",
        )
        .join(
            dim_flight.select(
                "flight_key",
                *flight_join_columns,
            ).alias("flight"),
            on=flight_join_columns,
            how="inner",
        )
        .select(
            F.col("booking.booking_id"),
            F.col("booking.event_id").alias("current_event_id"),
            F.col("date.date_key"),
            F.col("flight.flight_key"),
            F.col("booking.event_timestamp"),
            F.col("booking.booking_status"),
            F.col("booking.passenger_count"),
            F.col("booking.amount"),
            F.col("booking.currency"),
            F.col("booking.booking_channel"),
        )
    )


def build_gold(
    spark: SparkSession,
    silver_table: str,
    dim_date_table: str,
    dim_flight_table: str,
    fact_booking_table: str,
) -> None:
    silver_events = spark.table(silver_table)

    current_bookings = derive_current_booking_state(silver_events)
    dim_date = build_dim_date(current_bookings)
    dim_flight = build_dim_flight(current_bookings)
    fact_booking = build_fact_booking(
        current_bookings=current_bookings,
        dim_date=dim_date,
        dim_flight=dim_flight,
    )

    upsert_delta_table(
        spark=spark,
        updates=dim_date,
        table_name=dim_date_table,
        key_column="date_key",
    )

    upsert_delta_table(
        spark=spark,
        updates=dim_flight,
        table_name=dim_flight_table,
        key_column="flight_key",
    )

    upsert_delta_table(
        spark=spark,
        updates=fact_booking,
        table_name=fact_booking_table,
        key_column="booking_id",
    )

    print(f"Gold date dimension row count: {spark.table(dim_date_table).count()}")
    print(f"Gold flight dimension row count: {spark.table(dim_flight_table).count()}")
    print(f"Gold booking fact row count: {spark.table(fact_booking_table).count()}")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--dim-date-table", required=True)
    parser.add_argument("--dim-flight-table", required=True)
    parser.add_argument("--fact-booking-table", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    spark = SparkSession.builder.getOrCreate()

    build_gold(
        spark=spark,
        silver_table=args.silver_table,
        dim_date_table=args.dim_date_table,
        dim_flight_table=args.dim_flight_table,
        fact_booking_table=args.fact_booking_table,
    )


if __name__ == "__main__":
    main()
