from pyspark.sql import SparkSession
spark = SparkSession \
            .builder \
            .master('local') \
            .appName('appname') \
            .getOrCreate()

df = spark.read.option("Header", True).csv("D:\\testing\\Endorse_use.csv")
df = df.repartition(20)
df.write.mode("overwrite").csv("D:\\testing\\tst1")