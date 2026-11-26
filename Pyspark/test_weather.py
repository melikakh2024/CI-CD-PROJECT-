

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from weather import filtering

def test_filter():


    os.environ["PYHTHON_WORKER_SPARK"]=sys.executable
    os.environ["PYTHON_SPARK"]=sys.executable

    spark = SparkSession.builder \
    .appName("weather_test") \
    .master("local[*]") \
    .getOrCreate()

    data = [
    ("GM2000", "2025", "TMAX", 300, "0830"),
    ("FR2000", "2026", "TMAX", 200, None),
    ]
    columns = ["ID", "DATE", "ELEMENT", "VALUE", "OBS_TIME"]

    df = spark.createDataFrame(data, columns)

    row=filtering(df)
    rows=row.collect()

    assert len(rows)==1
    assert rows[0]["ID"]== "GM2000"