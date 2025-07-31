### 🧪 Run Lineage Job

Execute this inside the Spark container:

```bash
spark-submit \
  --conf spark.openlineage.transport.type=http \
  --conf spark.openlineage.transport.url=http://marquez:5000 \
  /opt/spark/app/spark_job.py
```

Then visit http://localhost:3000 to check the data lineage in the Marquez UI.