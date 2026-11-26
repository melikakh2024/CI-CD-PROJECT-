

from pyspark.sql import SparkSession
from pyspark.sql import functions as sf



def setup(path,spark):
    df=spark.read.text(path)
    return df


def trans(df):
    df=df.withColumn("ID",sf.substring(sf.col("value"),1,11))\
     .withColumn("LATITUDE",sf.substring(sf.col("value"),13,8))\
     .withColumn("LONGITUDE",sf.substring(sf.col("value"),22,8))\
     .withColumn("ELEVATION",sf.substring(sf.col("value"),32,6))\
     .drop(df.value)

    df = df.filter(
    sf.col("ID").startswith("GM") | sf.col("ID").startswith("AS") | sf.col("ID").startswith("AU"))

    return df

def output(df,out):
    df.write.mode("overwrite").parquet(out)


# if __name__=="__main__":

#     spark=SparkSession.builder\
#     .master("local[*]")\
#     .appName("APP")\
#     .getOrCreate()

#     df=setup(path,spark)
#     df=trans(df)
#     output(df,out)

#     spark.stop()