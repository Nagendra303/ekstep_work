from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

output_dir = sys.argv[1];

spark = SparkSession.builder.appName("EkStep_Assignment").getOrCreate();
df = spark.read.csv("/home/nagendra/Fire_Data/Fire_Department_Calls.csv", header = True,inferSchema = True)
df.select("Call Type").distinct();
df.coalesce(1).write.format("csv").option('header', "True").mode("overwrite").save(output_dir)

