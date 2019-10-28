from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("EkStep_Assignment").getOrCreate();
df = spark.read.csv("/home/nagendra/Fire_Data/Fire_Department_Calls.csv", header = True, inferSchema = True)
df1 = spark.read.csv("/home/nagendra/Fire_Data/Fire_Incidents_Data.csv", header=True, inferSchema=True)
df2 = df1.filter(df1['Neighborhood  District'].rlike('Nob Hill')).select('Incident Number','Neighborhood  District');
df_merge = df2.join(df,['Incident Number'],how='left');
df_merge.show();
#df_agg = df_merge.groupBy("Call Type").agg(F.countDistinct("Incident Number").alias('count'));
#df_agg.coalesce(1).limit(10).write.format("csv").option('header', "True").mode("overwrite").save("/home/nagendra/Anand_Task")



