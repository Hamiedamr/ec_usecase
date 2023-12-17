from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('example').getOrCreate()

df = spark.read.format('csv').option('header',True).load('menna.csv')
df.repartition(1).write.format('csv').option('index',False).mode('overwrite').save('output')