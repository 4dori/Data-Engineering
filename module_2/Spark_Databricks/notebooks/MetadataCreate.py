# Databricks notebook source
# MAGIC %pip install pydantic

# COMMAND ----------

from pydantic import BaseSettings
class Settings(BaseSettings): 
    SPARK_MASTER: str = "local[*]" 
    AZURE_ACC_1: str = "" 
    AZURE_CLIENT_ID_1: str = "" 
    AZURE_CLIENT_SECRET_1: str = "" 
    AZURE_TENANT_ID_1: str = "" 
    AZURE_ACC_2: str = "" 
    AZURE_ACC_KEY_2 = ""
    
    class Config: 
        env_file = ".env" 
        case_sensitive = True
    
settings = Settings()

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{settings.AZURE_ACC_1}.dfs.core.windows.net", 
               "OAuth") 
spark.conf.set( f"fs.azure.account.oauth.provider.type.{settings.AZURE_ACC_1}.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider", ) 
spark.conf.set( f"fs.azure.account.oauth2.client.id.{settings.AZURE_ACC_1}.dfs.core.windows.net", 
               settings.AZURE_CLIENT_ID_1, ) 
spark.conf.set( f"fs.azure.account.oauth2.client.secret.{settings.AZURE_ACC_1}.dfs.core.windows.net", 
               settings.AZURE_CLIENT_SECRET_1, ) 
spark.conf.set( f"fs.azure.account.oauth2.client.endpoint.{settings.AZURE_ACC_1}.dfs.core.windows.net", 
               settings.AZURE_TENANT_ID_1, )
spark.conf.set(f"fs.azure.account.key.{settings.AZURE_ACC_2}.dfs.core.windows.net", settings.AZURE_ACC_KEY_2)

# COMMAND ----------

df = spark.read.parquet(f"abfss://m07sparksql@{settings.AZURE_ACC_1}.dfs.core.windows.net/hotel-weather")  
av_df = spark.read.format("avro").load(f"abfss://m07sparksql@{settings.AZURE_ACC_1}.dfs.core.windows.net/expedia") 
df.write.mode("overwrite").format("delta").saveAsTable("hotel_weather")
av_df.write.format("delta").saveAsTable("expedia_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC # First part

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view max_dif_tmpr_hotels as
# MAGIC select *
# MAGIC from (select rank() over (partition by y_m order by avg_tmpr_c desc, id) as temp_rank,
# MAGIC   id, y_m, avg_tmpr_c
# MAGIC   from (
# MAGIC       select year || "_" || month as y_m, max(avg_tmpr_c) as avg_tmpr_c, id
# MAGIC       from hotel_weather
# MAGIC       group by y_m, id
# MAGIC   )
# MAGIC )
# MAGIC   where temp_rank <= 10

# COMMAND ----------

# MAGIC %md
# MAGIC # Second part

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view year_month_day
# MAGIC as select min(srch_ci) as min_d, max(srch_co) as max_d from expedia_delta

# COMMAND ----------

months_dif = int(spark.sql("select round(months_between(max_d, min_d)) as month_diff from year_month_day").first()["month_diff"])
spark.range(0, months_dif + 1).createOrReplaceTempView("months_dif")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view year_months
# MAGIC as select extract('year', (add_months(ymd.min_d, md.id))) || "-" || extract('month', (add_months(ymd.min_d, md.id))) as m
# MAGIC   from months_dif md, year_month_day ymd

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view mnth_cnt
# MAGIC as select e.hotel_id, ym.m, 1 as cnt
# MAGIC     from expedia_delta e
# MAGIC     join year_months ym
# MAGIC     on ym.m >= e.srch_ci and ym.m <= e.srch_co

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view busy_hotels as
# MAGIC select *  from (
# MAGIC   select row_number() over (partition by m order by cnt desc) as busy_rank,
# MAGIC     m, hotel_id, cnt
# MAGIC     from (
# MAGIC   select m, hotel_id, sum(cnt) as cnt
# MAGIC   from mnth_cnt
# MAGIC   group by m, hotel_id
# MAGIC )
# MAGIC )
# MAGIC where busy_rank <= 10
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Third part

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view lng_stay as 
# MAGIC select id, hotel_id, srch_ci, srch_co, datediff(srch_co, srch_ci) as st_d  from expedia_delta
# MAGIC where datediff(srch_co, srch_ci) >= 7

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view ech_day_tmpr as
# MAGIC select ls.id, hw.wthr_date, hw.avg_tmpr_c
# MAGIC from lng_stay ls
# MAGIC join hotel_weather hw
# MAGIC on ls.hotel_id = hw.id
# MAGIC where ls.srch_ci <= hw.wthr_date and ls.srch_co >= hw.wthr_date
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view avg_min_max_data as
# MAGIC select t1.*, t2.avg_tmpr_c as max_day_tmpr
# MAGIC from (select t1.*, t2.avg_tmpr_c as min_day_tmpr
# MAGIC from (
# MAGIC select id, 
# MAGIC avg(avg_tmpr_c) as avg_tmpr, 
# MAGIC min(wthr_date) as min_date,
# MAGIC max(wthr_date) as max_date
# MAGIC from ech_day_tmpr
# MAGIC group by id
# MAGIC ) t1
# MAGIC join ech_day_tmpr t2
# MAGIC on t1.id = t2.id
# MAGIC where t1.min_date = t2.wthr_date) t1
# MAGIC join ech_day_tmpr t2
# MAGIC on t1.id = t2.id
# MAGIC where t1.max_date = t2.wthr_date

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view tmpr_for_extnd
# MAGIC select ls.id, amd.avg_tmpr, 
# MAGIC case when ls.srch_ci = amd.min_date then min_day_tmpr
# MAGIC else null end as fd_tmpr,
# MAGIC case when ls.srch_co = amd.max_date then max_day_tmpr
# MAGIC else null end as ld_tmpr
# MAGIC from lng_stay ls
# MAGIC join avg_min_max_data amd
# MAGIC on ls.id = amd.id

# COMMAND ----------

df_1 = spark.sql("select * from max_dif_tmpr_hotels")
df_1.write.parquet(f"abfss://data@{settings.AZURE_ACC_2}.dfs.core.windows.net/module_2/task_1")
df_2 = spark.sql("select * from year_month_day")
df_2.write.parquet(f"abfss://data@{settings.AZURE_ACC_2}.dfs.core.windows.net/module_2/task_2")
df_3 = spark.sql("select * from tmpr_for_extnd")
df_3.write.parquet(f"abfss://data@{settings.AZURE_ACC_2}.dfs.core.windows.net/module_2/task_3")


