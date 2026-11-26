

from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

def setup(spark,file):
    df = spark.read.csv(file, header=False , schema=schema_m , dateFormat="yyyyMMdd")
    return df


def filtering(df):
    df1 = df.filter(
    sf.col("ID").startswith("GM") | sf.col("ID").startswith("AS") | sf.col("ID").startswith("AU"))
    df1=df1.select("ID", "DATE", "ELEMENT", "VALUE", "OBS_TIME")
    df1=df1.withColumn("OBS_TIME",sf.coalesce(sf.col("OBS_TIME"),sf.lit("0000")))\
    .withColumn("OBS_TIME",sf.concat(sf.substring(sf.col("OBS_TIME"),1,2),sf.lit(":"),sf.substring(sf.col("OBS_TIME"),3,2)))

    return df1



def output(out,df):

    df.write.mode("overwrite").parquet(out)










# if __name__=="__main__":
#     spark = SparkSession.builder \
#     .appName("weather_pipeline") \
#     .master("local[*]")\
#     .getOrCreate()


#     setup(spark,df)

#     spark.stop()

