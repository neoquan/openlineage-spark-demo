from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark with OpenLineage listener
spark = SparkSession.builder \
    .appName("lineage-demo") \
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener") \
    .config("spark.openlineage.transport.type", "http") \
    .config("spark.openlineage.url", "http://marquez:5000") \
    .config("spark.openlineage.namespace", "spark-docker") \
    .config("spark.openlineage.job.name", "lineage_demo_job") \
    .getOrCreate()

# === Simulated Inputs ===
customers = spark.read.option("header", True).csv("/opt/spark/data/customers.csv")
transactions = spark.read.option("header", True).csv("/opt/spark/data/transactions.csv")
regions = spark.createDataFrame([(10, "North"), (20, "South")], ["region_id", "region_name"])

# === Split 'name' into first and last ===
customers = customers.withColumn("first_name", split(col("name"), " ").getItem(0)) \
                     .withColumn("last_name", split(col("name"), " ").getItem(1))

# === Enrichment & Transformation ===
cust_txn = customers.join(transactions, "customer_id")

cust_txn_enriched = cust_txn \
    .join(regions, "region_id", "left") \
    .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name"))) \
    .withColumn("region_upper", upper(col("region_name"))) \
    .withColumn("amount_flag", when(col("amount") > 500, "HIGH").otherwise("LOW"))

# === Aggregation ===
agg = cust_txn_enriched \
    .filter(col("amount") > 100) \
    .groupBy("region_upper") \
    .agg(
        sum("amount").alias("total_amount"),
        sum(when(col("amount_flag") == "HIGH", col("amount")).otherwise(0)).alias("high_value_total")
    )

# === Output ===
agg.write.mode("overwrite").parquet("/tmp/output_agg")

spark.stop()
