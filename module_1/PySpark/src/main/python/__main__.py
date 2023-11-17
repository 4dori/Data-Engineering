from pyspark.sql import SparkSession
from sets import settings
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from fns import (
    replace_nulls_with_data,
    add_geohash_col_hotels_df,
    add_geohash_col_weather_df
)


# Spark session with account settings
spark = SparkSession.builder.appName("SimpleApp").config("spark.executor.memory", "1g").getOrCreate()

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

# schema for hotels csv file (name, type, nullable)
hotel_schema = StructType([ 
    StructField("Id", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("Latitude", FloatType(), True),
    StructField("Longitude", FloatType(), True)
])

# retriving hotels csv from Azure blob
hotels_df = spark.read \
    .option("nullValues", "null") \
    .schema(hotel_schema) \
    .csv(f"abfss://m06sparkbasics@{settings.AZURE_ACC_1}.dfs.core.windows.net/hotels")   

hotels_df = replace_nulls_with_data(hotels_df)
hotels_df = add_geohash_col_hotels_df(hotels_df)

# Retreive weather data from Azure Blob
weather_df = spark.read.parquet(f"abfss://m06sparkbasics@{settings.AZURE_ACC_1}.dfs.core.windows.net/weather")
weather_df = add_geohash_col_weather_df(weather_df)

# left join weather with hotels
final_df = weather_df.join(hotels_df, ["Geohash"], "left")

final_df.write.parquet(f"abfss://data@{settings.AZURE_ACC_2}.dfs.core.windows.net/module_2_output")

final_df = spark.read.parquet(f"abfss://data@{settings.AZURE_ACC_2}.dfs.core.windows.net/module_2_output")

final_df.show(10)

spark.stop()

