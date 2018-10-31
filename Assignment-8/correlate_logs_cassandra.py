from pyspark import SparkConf, SparkContext
import sys
import math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re, datetime, uuid
from pyspark.sql import SQLContext, Row, SparkSession, functions, types
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

def main(key_space,table):
	cluster_seeds = ['199.60.17.188', '199.60.17.216']
	spark = SparkSession.builder.appName('Spark Cassandra load table').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
	nasa_table = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=key_space).load()
	nasa_table = nasa_table.groupBy(nasa_table.host).agg(functions.count('*').alias('x'),functions.sum(nasa_table.bytes).alias('y')).drop('host')
	nasa_table = nasa_table.withColumn('n',functions.lit(1)).withColumn('x2',nasa_table.x * nasa_table.x).withColumn('y2',nasa_table.y * nasa_table.y).withColumn('xy',nasa_table.y * nasa_table.x)
	nasa_table = nasa_table.groupBy().sum()
	nasa_table = nasa_table.withColumn('r',(nasa_table['sum(n)'] * nasa_table['sum(xy)'] - nasa_table['sum(x)'] * nasa_table['sum(y)']) / (functions.sqrt(nasa_table['sum(n)'] * nasa_table['sum(x2)'] - nasa_table['sum(x)']**2) * functions.sqrt(nasa_table['sum(n)'] * nasa_table['sum(y2)'] - nasa_table['sum(y)']**2)))
	nasa_table = nasa_table.withColumn('r2',nasa_table.r**2)
	to_list = [list(row) for row in nasa_table.collect()]
	print(to_list[0][6],to_list[0][7])

if __name__ == "__main__":
    key_space = sys.argv[1]
    table = sys.argv[2]
    main(key_space,table)

 #spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11 correlate_logs_cassandra.py ehoque nasalogs