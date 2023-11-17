from main.python.fns import (
    replace_nulls_with_data,
    add_geohash_col_hotels_df,
    add_geohash_col_weather_df
)
from pyspark.sql import Row
from chispa.dataframe_comparer import assert_df_equality
import pytest

@pytest.fixture(scope="session")
def hotels_data(spark):
    data = [
        {"Id": 1, "Name": "ALM", "Country": "Kazakhstan", "City": "Almaty", "Address": "Mira st. 18","Latitude": None, "Longitude": None},
        {"Id": 2, "Name": "AST", "Country": "Kazakhstan", "City": "Astana", "Address": "Syganak st. 56","Latitude": None, "Longitude": None},
        {"Id": 3, "Name": "Holiday Inn Express", "Country":	"US", "City": "Ashland", "Address": "555 Clover Ln", "Latitude": 42.183544, "Longitude": -122.663345}
    ]
    df = spark.createDataFrame(Row(**x) for x in data)
    df.cache()
    df.count()
    return df

def test_replace_nulls_with_data(spark, hotels_data):
    expected_data = [
        {"Id": 1, "Name": "ALM", "Country": "Kazakhstan", "City": "Almaty", "Address": "Mira st. 18", 'Latitude': 76.9457275, 'Longitude': 43.2363924},
        {"Id": 2, "Name": "AST", "Country": "Kazakhstan", "City": "Astana", "Address": "Syganak st. 56", 'Latitude': 71.4306682, 'Longitude': 51.1282205},
        {"Id": 3, "Name": "Holiday Inn Express", "Country":	"US", "City": "Ashland", "Address": "555 Clover Ln", "Latitude": 42.183544, "Longitude": -122.663345}
    ]
    expected_result = spark.createDataFrame(Row(**x) for x in expected_data)
    result = replace_nulls_with_data(hotels_data)
    assert_df_equality(
        result, 
        expected_result, 
        ignore_column_order=True, 
        ignore_row_order=True
    )

@pytest.fixture(scope="session")
def hotels_data_gh(spark):
    data = [
        {"Id": 1, "Name": "ALM", "Country": "Kazakhstan", "City": "Almaty", "Address": "Mira st. 18", 'Latitude': 76.9457275, 'Longitude': 43.2363924},
        {"Id": 2, "Name": "AST", "Country": "Kazakhstan", "City": "Astana", "Address": "Syganak st. 56", 'Latitude': 71.4306682, 'Longitude': 51.1282205},
        {"Id": 3, "Name": "Holiday Inn Express", "Country":	"US", "City": "Ashland", "Address": "555 Clover Ln", "Latitude": 42.183544, "Longitude": -122.663345}
    ]
    df = spark.createDataFrame(Row(**x) for x in data)
    df.cache()
    df.count()
    return df

def test_add_geohash_col_hotels_df(spark, hotels_data_gh):
    expected_data = [
        {"Id": 1, "Name": "ALM", "Country": "Kazakhstan", "City": "Almaty", "Address": "Mira st. 18", 'Latitude': 76.9457275, 'Longitude': 43.2363924, 'Geohash': 'txwt'},
        {"Id": 2, "Name": "AST", "Country": "Kazakhstan", "City": "Astana", "Address": "Syganak st. 56", 'Latitude': 71.4306682, 'Longitude': 51.1282205, 'Geohash': 'v94f'},
        {"Id": 3, "Name": "Holiday Inn Express", "Country":	"US", "City": "Ashland", "Address": "555 Clover Ln", "Latitude": 42.183544, "Longitude": -122.663345, 'Geohash': 'hbjb'}
    ]
    expected_result = spark.createDataFrame(Row(**x) for x in expected_data)
    result = add_geohash_col_hotels_df(hotels_data_gh)
    assert_df_equality(
        result, 
        expected_result, 
        ignore_column_order=True, 
        ignore_row_order=True
    )
    
@pytest.fixture(scope="session")
def weather_data(spark):
    data = [
        {"lng": -111.09, "lat": 18.6251, "avg_tmpr_f": 80.7, "avg_tmpr_c": 27.1, "wthr_date": "2017-08-29", "wthr_year": "2017", "wthr_month": "8", "wthr_day": "29"},
        {"lng": -111.042, "lat": 18.6305, "avg_tmpr_f": 80.7, "avg_tmpr_c": 27.1, "wthr_date": "2017-08-29", "wthr_year": "2017", "wthr_month": "8", "wthr_day": "29"},
        {'lng': 43.2363924, 'lat': 79.9457275, "avg_tmpr_f": 80.7, "avg_tmpr_c": 27.1, "wthr_date": "2017-08-29", "wthr_year": "2017", "wthr_month": "8", "wthr_day": "29"}
    ]
    df = spark.createDataFrame(Row(**x) for x in data)
    df.cache()
    df.count()
    return df

def test_add_geohash_col_weather_df(spark, weather_data):
    expected_data = [
        {"lng": -111.09, "lat": 18.6251, "avg_tmpr_f": 80.7, "avg_tmpr_c": 27.1, "wthr_date": "2017-08-29", "wthr_year": "2017", "wthr_month": "8", "wthr_day": "29", 'Geohash': 'h2j0'},
        {"lng": -111.042, "lat": 18.6305, "avg_tmpr_f": 80.7, "avg_tmpr_c": 27.1, "wthr_date": "2017-08-29", "wthr_year": "2017", "wthr_month": "8", "wthr_day": "29", 'Geohash': 'h2j0'},
        {'lng': 43.2363924, 'lat': 79.9457275, "avg_tmpr_f": 80.7, "avg_tmpr_c": 27.1, "wthr_date": "2017-08-29", "wthr_year": "2017", "wthr_month": "8", "wthr_day": "29", 'Geohash': 'tz8v'}
    ]
    expected_result = spark.createDataFrame(Row(**x) for x in expected_data)
    result = add_geohash_col_weather_df(weather_data)
    assert_df_equality(
        result, 
        expected_result, 
        ignore_column_order=True, 
        ignore_row_order=True
    )