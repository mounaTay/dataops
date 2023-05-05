import pyspark
from delta.tables import DeltaTable
from pyspark.sql.functions import *

TABLE_NAME = "table"
BRONZE_LAYER_DATA_PATH = "data"
SILVER_LAYER_DTA_PATH = f"deltas/{TABLE_NAME}"
GOLD_LAYER_DATA_PATH = f"output/{TABLE_NAME}"


def get_spark_session() -> pyspark.sql.SparkSession:
    return (
        pyspark.sql.SparkSession.builder.appName("Project Demo")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "4")
        .getOrCreate()
    )


def create_snapshot(data_path: str = BRONZE_LAYER_DATA_PATH, **context) -> None:
    spark = get_spark_session()
    df = spark.read.option("header", True).option("inferSchema", True).csv(data_path)
    # Add integration time, year and month to dataframe
    df = (
        df.withColumn("integration_time", current_timestamp())
        .withColumn("year", year("integration_time"))
        .withColumn("month", month("integration_time"))
    )
    # Take a snapshot of the data
    df.write.partitionBy("year", "month").format("delta").mode("overwrite").option(
        "mergeSchema", True
    ).save(SILVER_LAYER_DTA_PATH)
    context["ti"].xcom_push(key="first_run", value=True)


def update_data(data_path: str = BRONZE_LAYER_DATA_PATH) -> None:
    spark = get_spark_session()
    snapshot = DeltaTable.forPath(spark, SILVER_LAYER_DTA_PATH)
    updates = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(data_path)
        .withColumn("integration_time", current_timestamp())
        .withColumn("year", year("integration_time"))
        .withColumn("month", month("integration_time"))
    )

    snapshot.alias("snapshot").merge(
        updates.alias("updates"),
        "snapshot.id = updates.id",
    ).whenMatchedUpdateAll(
        "snapshot.integration_time <= updates.integration_time"
    ).whenNotMatchedInsertAll().execute()


def process_data(data_path: str) -> None:
    spark = get_spark_session()

    data = DeltaTable.forPath(spark, data_path).toDF()
    data.groupBy("name", "year", "month").agg(
        sum("price").alias("price"), collect_set("id").alias("ids")
    )
    data.write.partitionBy("year", "month").format("delta").mode("overwrite").option(
        "mergeSchema", True
    ).save(GOLD_LAYER_DATA_PATH)
