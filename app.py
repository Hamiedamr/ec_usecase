import os
os.system('hdfs dfs -put /root/ec_usecase/menna.csv /user/admin')

from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('test').getOrCreate()
#Read CSV file from hdfs path
df = spark.read.option("header",True).csv('hdfs:////user/admin/menna.csv')
new_df = (df
      .withColumnRenamed('VIN (1-10)', 'VIN Score')
      .withColumn('Postal Code',col('Postal Code').cast('float'))
      .withColumn('Model Year',col('Model Year').cast('int'))
      .withColumn('Electric Range',col('Electric Range').cast('float'))
      .withColumn('Base MSRP',col('Base MSRP').cast('float'))
      .withColumn('Legislative District',col('Legislative District').cast('float'))
      .withColumn('DOL Vehicle ID',col('DOL Vehicle ID').cast('float'))
      )
ec_per_country = new_df.groupBy('City').agg(count('City').alias('Car_Per_City'))
sorted_ec_per_country = ec_per_country.orderBy(desc('Car_Per_City'))
sorted_ec_per_country.repartition(1).write.mode('overwrite').option("header",True).csv('hdfs:////user/admin/menna_sorted.csv')

os.system("hdfs dfs -get /user/admin/menna_sorted.csv /root/ec_usecase/output")
# print(sorted_ec_per_country.show())