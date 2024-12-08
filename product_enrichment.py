from main import *


def transformations(app, df):
    valid_file = app.read.csv('valid_table\part-00000-4f5ef94f-972b-4e40-93e4-89975aedefb7-c000.csv', header=True, inferSchema=True)
    product_enrichment_table = valid_file.filter(valid_file['USI Code'] == df['USI'])
    product_enrichment_table.show(4)


def main():
    app = SparkApplication('Product_enrichment')
    spark = app.get_spark()

    product_file = spark.read('product_source.csv', header=True)

    transformations(spark, product_file)

    spark.stop()


if __name__ == '__main__':
    main()


