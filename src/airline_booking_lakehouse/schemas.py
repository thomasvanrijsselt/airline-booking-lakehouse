from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BOOKING_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=True),
        StructField("event_timestamp", TimestampType(), nullable=True),
        StructField("booking_id", StringType(), nullable=True),
        StructField("flight_number", StringType(), nullable=True),
        StructField("departure_airport", StringType(), nullable=True),
        StructField("arrival_airport", StringType(), nullable=True),
        StructField("departure_date", DateType(), nullable=True),
        StructField("passenger_count", IntegerType(), nullable=True),
        StructField("amount", DecimalType(10, 2), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("booking_channel", StringType(), nullable=True),
    ]
)