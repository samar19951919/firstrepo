from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum


def run_pipeline():
    """Run a simple PySpark transformation pipeline."""
    spark = SparkSession.builder.appName("TransformationPipeline").master("local[*]").getOrCreate()

    # Sample data for demonstration purposes
    data = [
        ("A", 1),
        ("B", 2),
        ("A", 3),
    ]
    df = spark.createDataFrame(data, ["category", "value"])

    # Filter rows where value is greater than 1 and aggregate
    result = (
        df.filter(col("value") > 1)
          .groupBy("category")
          .agg(_sum("value").alias("total_value"))
    )

    result.show()
    spark.stop()


if __name__ == "__main__":
    run_pipeline()
