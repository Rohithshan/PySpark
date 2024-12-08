from main import *


def main():
    app = SparkApplication('Product_enrichment')
    spark = app.get_spark()


if __name__ == '__main__':
    main()


