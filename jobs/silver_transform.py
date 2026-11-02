"""Databricks batch entry point for normalizing trip records into Delta Silver."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def transform(source: DataFrame) -> tuple[DataFrame, DataFrame]:
    required = {
        "trip_id",
        "service_date",
        "route_id",
        "scheduled_departure",
        "actual_departure",
        "scheduled_arrival",
        "actual_arrival",
        "passenger_count",
    }
    missing = required - set(source.columns)
    if missing:
        raise ValueError(f"source is missing required columns: {', '.join(sorted(missing))}")

    normalized = source.select(
        F.trim("trip_id").alias("trip_id"),
        F.to_date("service_date").alias("service_date"),
        F.trim("route_id").alias("route_id"),
        F.to_timestamp(
            F.concat(F.lit("1970-01-01 "), F.trim("scheduled_departure")),
            "yyyy-MM-dd HH:mm",
        ).alias("scheduled_departure"),
        F.to_timestamp(
            F.concat(F.lit("1970-01-01 "), F.trim("actual_departure")),
            "yyyy-MM-dd HH:mm",
        ).alias("actual_departure"),
        F.to_timestamp(
            F.concat(F.lit("1970-01-01 "), F.trim("scheduled_arrival")),
            "yyyy-MM-dd HH:mm",
        ).alias("scheduled_arrival"),
        F.to_timestamp(
            F.concat(F.lit("1970-01-01 "), F.trim("actual_arrival")),
            "yyyy-MM-dd HH:mm",
        ).alias("actual_arrival"),
        F.col("passenger_count").cast("integer").alias("passenger_count"),
        F.monotonically_increasing_id().alias("_source_row_id"),
    )
    duplicate_window = Window.partitionBy("trip_id").orderBy("_source_row_id")
    normalized = normalized.withColumn("_duplicate_rank", F.row_number().over(duplicate_window))

    invalid = (
        F.col("trip_id").isNull()
        | (F.col("trip_id") == "")
        | F.col("service_date").isNull()
        | F.col("route_id").isNull()
        | (F.col("route_id") == "")
        | F.col("scheduled_departure").isNull()
        | F.col("actual_departure").isNull()
        | F.col("scheduled_arrival").isNull()
        | F.col("actual_arrival").isNull()
        | F.col("passenger_count").isNull()
        | (F.col("passenger_count") < 0)
        | (F.col("_duplicate_rank") > 1)
    )
    rejected = normalized.filter(invalid).withColumn(
        "rejection_reason",
        F.when(F.col("_duplicate_rank") > 1, "duplicate trip_id").otherwise(
            "required value invalid or missing"
        ),
    )
    valid = normalized.filter(~invalid).drop("_source_row_id", "_duplicate_rank")
    scheduled_arrival_offset = (
        F.hour("scheduled_arrival") * 60
        + F.minute("scheduled_arrival")
        + F.when(F.col("scheduled_arrival") < F.col("scheduled_departure"), 1440).otherwise(0)
    )
    actual_arrival_offset = (
        F.hour("actual_arrival") * 60
        + F.minute("actual_arrival")
        + F.when(F.col("actual_arrival") < F.col("actual_departure"), 1440).otherwise(0)
    )
    delay = actual_arrival_offset - scheduled_arrival_offset
    valid = valid.withColumn("arrival_delay_minutes", delay.cast("integer")).withColumn(
        "on_time", F.col("arrival_delay_minutes") <= 5
    )
    return valid, rejected


if __name__ == "__main__":
    spark = SparkSession.builder.appName("trip-silver-transform").getOrCreate()
    source_path = spark.conf.get("pipeline.source_path")
    silver_path = spark.conf.get("pipeline.silver_path")
    quarantine_path = spark.conf.get("pipeline.quarantine_path")
    source = spark.read.option("header", True).csv(source_path)
    silver, rejected = transform(source)
    silver.write.format("delta").mode("append").partitionBy("service_date").save(silver_path)
    rejected.write.format("delta").mode("append").save(quarantine_path)
