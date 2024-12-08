from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


class SparkApplication():
    def __init__(self, appname):
        self.spark = SparkSession.builder.appName(appname).getOrCreate()

    def get_spark(self):
        return self.spark





