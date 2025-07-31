from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("lineage-demo") \
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener") \
    .config("spark.openlineage.transport.type", "http") \
    .config("spark.openlineage.url", "http://marquez:5000") \
    .config("spark.openlineage.namespace", "spark-docker") \
    .config("spark.openlineage.job.name", "demo_job") \
    .getOrCreate()

df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.write.mode("overwrite").csv("/tmp/output")
spark.stop()
