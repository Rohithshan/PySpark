from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = SparkSession.builder.appName("Pyspark_example").getOrCreate()

df = spark.read.csv('telecom_details.csv', header=True, inferSchema=True)

error_table = df.filter(df['Contract Start Date'].isNull() | df['Contract End Date'].isNull())\

print(df[4])

valid_table = df.subtract(error_table).withColumn(
    "Product Name",
    when(df['Product Name'].isNull(), "ItNULL").otherwise(df['Product Name'])
).withColumn(
    "Product ID",
    when(col('Product Name') == 'Phone', '3d541dcd')
    .when(col('Product Name') == 'SmartWatch', 'bb28fb32')
    .when(col('Product Name') == 'Tablet', '01dc1d3a')
    .otherwise('000AA')
)

error_table.coalesce(1).write.mode('overwrite').csv('error_table', header=True)
valid_table.coalesce(1).write.mode('overwrite').csv('valid_table', header=True)

print(error_table.count())
print(valid_table.count())

print("Error table success")
spark.stop()

