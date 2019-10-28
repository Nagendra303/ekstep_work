from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

output_dir = sys.argv[1]

spark = SparkSession.builder.appName("EkStep_Assignment").getOrCreate();
df = spark.read.csv("/home/nagendra/Fire_Data/Fire_Department_Calls.csv", header = True, inferSchema = True)
df1 = df.groupBy("Call Type").agg(F.countDistinct("Incident Number").alias('count'));
df2 = df1.orderBy("count", ascending=False)
df2.coalesce(1).limit(10).write.format("csv").option('header', "True").mode("overwrite").save(output_dir)
