from pyspark import SparkConf, SparkContext
import sys
import math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re, datetime
from pyspark.sql import SQLContext, Row, SparkSession, functions, types
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType


schema = StructType([
    StructField('hostname', StringType(), False),
    StructField('path', StringType(), False),
    StructField('size', FloatType(), False),
    StructField('timestamp', TimestampType(), False)
])

def word_split(line):
    split_words = linere.split(line)
    return split_words

def filter_len(line):
    return len(line) > 4

def create_rdd(lineSplit):
    return Row(hostname=lineSplit[1],timestamp=datetime.datetime.strptime(lineSplit[2], '%d/%b/%Y:%H:%M:%S'),path=lineSplit[3], size=float(lineSplit[4]))

def main(inputs):
    text = sc.textFile(inputs)
    nasa_rdd = text.map(word_split).filter(filter_len).map(create_rdd)
    nasa_table = sqlContext.createDataFrame(nasa_rdd, schema)
    nasa_table = nasa_table.groupBy(nasa_table.hostname).agg(functions.count('*').alias('x'),functions.sum(nasa_table.size).alias('y')).drop('hostname')
    nasa_table = nasa_table.withColumn('n',functions.lit(1)).withColumn('x2',nasa_table.x * nasa_table.x).withColumn('y2',nasa_table.y * nasa_table.y).withColumn('xy',nasa_table.y * nasa_table.x)
    nasa_table = nasa_table.groupBy().sum()
    nasa_table = nasa_table.withColumn('r',(nasa_table['sum(n)'] * nasa_table['sum(xy)'] - nasa_table['sum(x)'] * nasa_table['sum(y)']) / (functions.sqrt(nasa_table['sum(n)'] * nasa_table['sum(x2)'] - nasa_table['sum(x)']**2) * functions.sqrt(nasa_table['sum(n)'] * nasa_table['sum(y2)'] - nasa_table['sum(y)']**2)))
    nasa_table = nasa_table.withColumn('r2',nasa_table.r**2)
    to_list = [list(row) for row in nasa_table.collect()]
    print(to_list[0][6],to_list[0][7])


if __name__ == '__main__':
    conf = SparkConf().setAppName('nasa-log-ingest')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")
    inputs = sys.argv[1]
    main(inputs)