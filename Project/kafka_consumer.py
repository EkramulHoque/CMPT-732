from pyspark import SparkConf, SparkContext
import sys
import math

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import re, datetime, uuid
from pyspark.sql import SQLContext, Row, SparkSession, functions, types
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

spark = SparkSession.builder.appName('Read Client data').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def main(topic):
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
        .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))
    stream = values.writeStream.format('console').outputMode('update').start()
    stream.awaitTermination(200)

if __name__ == "__main__":
    topic = sys.argv[1]
    main(topic)
