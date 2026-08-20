from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.getOrCreate()

    print("Hello from the Airline Booking Lakehouse!")

    spark.range(1, 4).show()


if __name__ == "__main__":
    main()