
from pyspark.sql import SparkSession
from country import convert


def test_convert_keeps_only_three_countries():

    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    data = [("AU Austria",), ("GM Germany",), ("AS Australia",), ("FR France",)]
    df = spark.createDataFrame(data, ["value"])
    result = convert(df)
    rows = result.collect()
    ids = [row["ID"] for row in rows]

    assert "AS" in ids
    assert "FR" not in ids




