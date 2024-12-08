from main import *


def transformations(app, valid_file, product):
    # valid_file.show(4)
    # valid_file.withColumnRenamed('USI Code', 'USI')
    product_enrichment_table = valid_file.join(
        product,
        valid_file['USI Code'] == product['USI_source'],
        how='inner'
    )
    # product_enrichment_table.show(4)
    return product_enrichment_table


def main():
    app = SparkApplication('Product_enrichment')
    spark = app.get_spark()

    product_source = spark.read.csv('product_source.csv', header=True, inferSchema=True)
    valid_file = spark.read.csv('valid_table\part-00000-4f5ef94f-972b-4e40-93e4-89975aedefb7-c000.csv', header=True,inferSchema=True)

    rename_cols = [col(c).alias(c + '_source') for c in product_source.columns]
    product_source = product_source.select(*rename_cols)

    product_enrich = transformations(spark, valid_file, product_source)

    app.file_write(product_enrich, 'product_enriched')

    spark.stop()


if __name__ == '__main__':
    main()


