from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Unit_Testing").getOrCreate()
df = spark.read.csv("/home/nagendra/Fire_Data/Fire_Department_Calls.csv", header=True, inferSchema= True)
df1 = df.select("Call Type").count()
print("Total Count: ", df1)
df2 = df.agg(F.countDistinct("Call Type"))
df2.show()
df3 = df.filter(F.col("Call Type").isNull()).count()
print("Null Value Count: ",df3)
df4 = df.filter(F.col("Call Type").isNotNull()).count()
print("Not Null Value Count: ",df4)