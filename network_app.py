from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Network APP').getOrCreate()

# Read the text file
network_activity_dpi_protocol_df = spark.read.csv("/root/ec_usecase/NETWORK_ACTIVITY_DPI_PROTOCOL.csv",sep=';', header=True, inferSchema=True)

network_activity_dpi_url_df = spark.read.csv("/root/ec_usecase/NETWORK_ACTIVITY_DPI_URL.csv",sep=';', header=True, inferSchema=True)

mutual_columns = list(set(network_activity_dpi_protocol_df.columns).intersection(set(network_activity_dpi_url_df.columns)))

joined_df = network_activity_dpi_protocol_df.join(network_activity_dpi_url_df, on=mutual_columns[0])
print(joined_df.show())

