# Databricks notebook source
from pyspark.sql.window import Window
from delta.tables import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
ppath="dbfs:/mnt/output"
schema=spark.read.format("parquet").option("header", "true").load(ppath).schema
df=spark.readStream.schema(schema).format("parquet").option("header", "true").load(ppath)
df.isStreaming

# COMMAND ----------

from pyspark.sql import functions as F

df_task_1 = df.select(col("city"), col("wthr_date"), col("name"), col("avg_tmpr_c"))\
    .groupBy(window(col("wthr_date"), "1 day"), col("city"))\
    .agg(F.approx_count_distinct("name").alias("hotel_count"), F.avg("avg_tmpr_c").alias("avg_tmpr"))\
    .orderBy(col("window.start"))
df_task_1.printSchema()
df_task_1.writeStream\
    .outputMode("complete")\
    .option("checkpointLocation", "/mnt/tmp/checkpoint_t1")\
    .toTable("task_1")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM task_1

# COMMAND ----------

top_10_df = df.groupBy("city") \
    .agg(F.approx_count_distinct("name").alias("distinct_hotel_count")) \
    .orderBy(F.desc("distinct_hotel_count")) \
    .limit(10)
top_10_df.isStreaming


# COMMAND ----------

top_10_df\
    .writeStream\
    .outputMode("complete")\
    .option("checkpointLocation", "/mnt/tmp/checkpoint")\
    .toTable("top_10_h")

# COMMAND ----------

incoming_data_df = spark.sql("select * from top_10_h")

# COMMAND ----------

lst = incoming_data_df.rdd.map(lambda x: x.city).collect()
print(lst)

# COMMAND ----------

for i in range(10):
    df.filter(df.city == lst[i])\
    .createOrReplaceTempView(f"city_weather_{i}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM city_weather_0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM city_weather_1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM city_weather_2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM city_weather_3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM city_weather_4

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM city_weather_5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM city_weather_6

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM city_weather_7

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM city_weather_8

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM city_weather_9
