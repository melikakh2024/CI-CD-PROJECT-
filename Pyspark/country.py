
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

def setup(path,spark):
    df=spark.read.text(path)
    return df


def convert (df):
    df=df.withColumn("ID", sf.substring(sf.col("value"),1 ,2))\
        .withColumn("country", sf.substring(sf.col("value"),4,61))\
        .drop("value")



    target=["GM","AU","AS"]
    df=df.filter(sf.col("ID").isin(target))
    return df



def output(path,df ):
    df.write.parquet(path)
    return  path




if __name__=="__main__":

    spark=SparkSession.builder\
    .master("local[*]")\
    .appName("APP")\
    .getOrCreate()

    df=setup("./sources/countries.txt",spark)

    df=convert(df)

    output("./output/country",df)
