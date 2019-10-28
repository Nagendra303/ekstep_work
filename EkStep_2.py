from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
import sys
output_dir = sys.argv[1]

spark = SparkSession.builder.appName("EkStep_Assignment").getOrCreate();
df = spark.read.csv("/home/nagendra/Fire_Data/Fire_Department_Calls.csv", header = True, inferSchema = True)
df1    = df.filter(df['Call Type'].rlike(".*Fire.*"));
df_agg_overall = df.agg(F.countDistinct('Call Date').alias('count'));
df_agg_overall.coalesce(1).write.format("csv").option('header', "True").mode("overwrite").save(output_dir)



