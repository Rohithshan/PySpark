from main import *


def file_write(filename, filepath):
    filename.coalesce(1).write.mode('overwrite').csv(filepath, header=True)


def transformations(df):
    error_table = df.filter(df['Contract Start Date'].isNull() | df['Contract End Date'].isNull())
    valid_table = df.subtract(error_table).withColumn(
        "Product Name",
        when(df['Product Name'].isNull(), "ItNULL").otherwise(df['Product Name'])
    ).withColumn(
        "Product ID",
        when(col('Product Name')=='Phone', '3d541dcd')
        .when(col('Product Name')=='SmartWatch', 'bb28fb32')
        .when(col('Product Name')=='Tablet', '01dc1d3a')
        .otherwise('000AA')
    )
    file_write(error_table, 'error_table')
    return valid_table


def main():
    app = SparkApplication('valid_file')
    spark = app.get_spark()

    df = spark.read.csv('telecom_details.csv', header=True, inferSchema=True)
    valid_table = transformations(df)
    file_write(valid_table, 'valid_table')

    spark.stop()


if __name__ == '__main__':
    main()