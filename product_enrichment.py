from pyspark.sql import SparkSession


class SparkApplication():
    def __init__(self, appname):
        spark = SparkSession.builder.appName(appname).getOrCreate()

        df = spark.read.csv('product_source.csv')

        df.show(10)


def main():
    spark = SparkApplication('Product_enrichment')
    print('passed')



if __name__ == '__main__':
    main()


