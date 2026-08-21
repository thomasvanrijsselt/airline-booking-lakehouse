import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from airline_booking_lakehouse.schemas import BOOKING_EVENT_SCHEMA


def read_incremental_events(
    spark: SparkSession,
    source_path: str,
) -> DataFrame:
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(BOOKING_EVENT_SCHEMA)
        .load(source_path)
    )


def add_ingestion_metadata(events: DataFrame) -> DataFrame:
    return (
        events.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
    )


def ingest_bronze(
    spark: SparkSession,
    source_path: str,
    checkpoint_path: str,
    table_name: str,
) -> None:
    raw_events = read_incremental_events(spark, source_path)
    bronze_events = add_ingestion_metadata(raw_events)

    query = (
        bronze_events.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(table_name)
    )

    query.awaitTermination()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--table-name", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    spark = SparkSession.builder.getOrCreate()

    ingest_bronze(
        spark=spark,
        source_path=args.source_path,
        checkpoint_path=args.checkpoint_path,
        table_name=args.table_name,
    )

    row_count = spark.table(args.table_name).count()
    print(f"Bronze row count: {row_count}")


if __name__ == "__main__":
    main()
