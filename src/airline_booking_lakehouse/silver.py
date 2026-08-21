import argparse

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

VALID_EVENT_TYPES = [
    "BOOKING_CREATED",
    "BOOKING_UPDATED",
    "BOOKING_CANCELLED",
]

QUARANTINE_KEY_COLUMNS = [
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
    "_source_file",
]


def normalize_events(events: DataFrame) -> DataFrame:
    return (
        events.withColumn("event_id", F.trim("event_id"))
        .withColumn("event_type", F.upper(F.trim("event_type")))
        .withColumn("booking_id", F.trim("booking_id"))
        .withColumn("flight_number", F.upper(F.trim("flight_number")))
        .withColumn(
            "departure_airport",
            F.upper(F.trim("departure_airport")),
        )
        .withColumn(
            "arrival_airport",
            F.upper(F.trim("arrival_airport")),
        )
        .withColumn("currency", F.upper(F.trim("currency")))
        .withColumn("booking_channel", F.lower(F.trim("booking_channel")))
    )


def rule_passes(condition: Column) -> Column:
    return F.coalesce(condition, F.lit(False))


def apply_quality_rules(events: DataFrame) -> DataFrame:
    rules = {
        "event_id_required": F.col("event_id").isNotNull() & (F.length("event_id") > 0),
        "booking_id_required": F.col("booking_id").isNotNull()
        & (F.length("booking_id") > 0),
        "valid_event_type": F.col("event_type").isin(VALID_EVENT_TYPES),
        "event_timestamp_required": F.col("event_timestamp").isNotNull(),
        "departure_date_required": F.col("departure_date").isNotNull(),
        "valid_departure_airport": F.col("departure_airport").rlike("^[A-Z]{3}$"),
        "valid_arrival_airport": F.col("arrival_airport").rlike("^[A-Z]{3}$"),
        "different_airports": F.col("departure_airport") != F.col("arrival_airport"),
        "positive_passenger_count": F.col("passenger_count") > 0,
        "non_negative_amount": F.col("amount") >= 0,
        "currency_is_eur": F.col("currency") == "EUR",
    }

    failed_rules = F.array_compact(
        F.array(
            *[
                F.when(
                    ~rule_passes(condition),
                    F.lit(rule_name),
                )
                for rule_name, condition in rules.items()
            ]
        )
    )

    return events.withColumn("failed_rules", failed_rules).withColumn(
        "is_valid",
        F.size("failed_rules") == 0,
    )


def deduplicate_events(events: DataFrame) -> DataFrame:
    duplicate_window = Window.partitionBy("event_id").orderBy(
        F.col("_ingested_at").desc(),
        F.col("event_timestamp").desc(),
        F.col("_source_file").desc(),
    )

    return (
        events.withColumn(
            "_deduplication_rank",
            F.row_number().over(duplicate_window),
        )
        .filter(F.col("_deduplication_rank") == 1)
        .drop("_deduplication_rank")
    )


def split_by_quality(
    events: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    valid_events = events.filter(F.col("is_valid"))
    invalid_events = events.filter(~F.col("is_valid"))

    return valid_events, invalid_events


def add_quarantine_id(events: DataFrame) -> DataFrame:
    key_values = [
        F.coalesce(
            F.col(column_name).cast("string"),
            F.lit("<null>"),
        )
        for column_name in QUARANTINE_KEY_COLUMNS
    ]

    return events.withColumn(
        "quarantine_id",
        F.sha2(F.concat_ws("||", *key_values), 256),
    )


def upsert_delta_table(
    spark: SparkSession,
    updates: DataFrame,
    table_name: str,
    key_column: str,
) -> None:
    if key_column not in updates.columns:
        raise ValueError(
            f"Merge key {key_column!r} is missing from the source DataFrame"
        )

    contains_null_key = updates.filter(F.col(key_column).isNull()).limit(1).count() > 0
    if contains_null_key:
        raise ValueError(f"Merge key {key_column!r} must not contain null values")

    if not spark.catalog.tableExists(table_name):
        (updates.write.format("delta").mode("ignore").saveAsTable(table_name))
        return

    from delta.tables import DeltaTable

    target_table = DeltaTable.forName(spark, table_name)
    merge_condition = f"target.`{key_column}` = source.`{key_column}`"

    (
        target_table.alias("target")
        .merge(
            updates.alias("source"),
            merge_condition,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def build_silver(
    spark: SparkSession,
    bronze_table: str,
    silver_table: str,
    quarantine_table: str,
) -> None:
    bronze_events = spark.table(bronze_table)

    normalized_events = normalize_events(bronze_events)
    checked_events = apply_quality_rules(normalized_events)
    valid_events, invalid_events = split_by_quality(checked_events)

    silver_updates = deduplicate_events(valid_events).drop(
        "failed_rules",
        "is_valid",
    )

    quarantine_updates = add_quarantine_id(invalid_events).dropDuplicates(
        ["quarantine_id"]
    )

    upsert_delta_table(
        spark=spark,
        updates=silver_updates,
        table_name=silver_table,
        key_column="event_id",
    )
    upsert_delta_table(
        spark=spark,
        updates=quarantine_updates,
        table_name=quarantine_table,
        key_column="quarantine_id",
    )

    silver_count = spark.table(silver_table).count()
    quarantine_count = spark.table(quarantine_table).count()

    print(f"Silver row count: {silver_count}")
    print(f"Quarantine row count: {quarantine_count}")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--quarantine-table", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    spark = SparkSession.builder.getOrCreate()

    build_silver(
        spark=spark,
        bronze_table=args.bronze_table,
        silver_table=args.silver_table,
        quarantine_table=args.quarantine_table,
    )


if __name__ == "__main__":
    main()
