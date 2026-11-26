


from pyspark.sql import SparkSession
from station import  trans
import os
import sys

def test_trans():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("TEST") \
        .getOrCreate()


    data = [
        ("GM123456789 12.34567 009.8765  000500",),
        ("AS987654321 48.12345 014.2345  001200",),
        ("XX000000000 10.00000 020.0000  000100",),
    ]

    df_test = spark.createDataFrame(data, ["value"])

    df=trans(df_test)
    row=df.collect()


    assert len(row)==2
    assert row[0]["ID"]=="GM123456789"