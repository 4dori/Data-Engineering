from opencage.geocoder import OpenCageGeocode
import pygeohash as pgh
from pyspark.sql.functions import monotonically_increasing_id, col


# geocoder session
key = 'ba428293f5b34562b6ae2f247fedd0f2'
geocoder = OpenCageGeocode(key)

# retreives "lng" and "lat" from geocoder using address
def add_location(row):
    result = geocoder.geocode(row["Place"])
    lng = result[0]["geometry"]["lat"]
    lat = result[0]["geometry"]["lng"]
    return{"Id": row["Id"], "Longitude": lng, "Latitude": lat}

def replace_nulls_with_data(hotels_df):
    hotels_with_null = hotels_df.filter(col("Latitude").isNull()) \
        .select("Id", "Name", "Country", "City", "Address") 
    # create df with "Id", "Lngitude", and "Latitude"
    hotels_with_null_rdd = hotels_with_null.rdd \
        .map(lambda x: {"Place": x.Country + ', ' + x.City, "Id": x.Id})
    hotels_null_replaced_df = hotels_with_null_rdd.map(add_location).toDF()
    hotels_nulls_replaced_df = hotels_with_null.join(hotels_null_replaced_df, ["ID"], "inner")
    hotels_full = hotels_df \
        .filter(col("Latitude").isNotNull()) \
        .union(hotels_nulls_replaced_df)
    return hotels_full

def generate_geohash(row):
    geohash = pgh.encode(row["Longitude"], row["Latitude"], precision=4)
    return {"Id": row["Id"], "Geohash": geohash}


def add_geohash_col_hotels_df(hotels_full):
    # create df with "Id" and "Geohash"
    hotels_loc_rdd= hotels_full.rdd.map(lambda x: {"Id":x.Id, "Latitude": x.Latitude, "Longitude": x.Longitude})
    hotels_geohash_df = hotels_loc_rdd.map(generate_geohash).toDF()
    hotels_final = hotels_full.join(hotels_geohash_df, ["Id"], "inner")
    return hotels_final

def add_geohash_col_weather_df(weather_df):
    # add "Id" row as a primary key for weather data
    weather_df = weather_df.withColumn("Id", monotonically_increasing_id())
    # add geohash col to weather
    weather_geohash_rdd = weather_df.rdd.map(lambda x: {"Id": x.Id, "Longitude": x.lng, "Latitude": x.lat})
    weather_geohash_df = weather_geohash_rdd.map(generate_geohash).toDF()
    weather_df_full = weather_df.join(weather_geohash_df, ["Id"], "Inner")
    # remove primary key
    weather_df_full = weather_df_full.drop("Id")
    return weather_df_full