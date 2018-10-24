from pyspark import SparkConf, SparkContext
import sys
import math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re, datetime, uuid
from pyspark.sql import SQLContext, Row, SparkSession, functions, types
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

def log_map(line):
    words = linex.split(line)
    if(len(words) >= 4):
        yield ({'id':str(uuid.uuid1()), 'host':words[1], 'datetime': datetime.datetime.strptime(words[2], '%d/%b/%Y:%H:%M:%S'), 'path': words[3], 'bytes': float(words[4])})

def main(inputs,key_space,table):
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('Load Spark').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    sc = spark.sparkContext
    text = sc.textFile(inputs) 
    partition_count = NUM_EXECUTOR * 3
    text = text.repartition(partition_count)
    result = text.flatMap(log_map).toDF()
    result.write.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=key_space).save()

if __name__ == "__main__":
    inputs = sys.argv[1]
    key_space = sys.argv[2]
    table = sys.argv[3]
    linex = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")
    NUM_EXECUTOR = 50
    main(inputs,key_space,table)
