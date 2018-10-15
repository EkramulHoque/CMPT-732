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

def r2_reduce(kv):
    key, value = kv
    return (key+'2',value*value)

def corr_func(kv):
    key, sixsum = kv
    return  (key, (sixsum[0]*sixsum[5] - sixsum[1]*sixsum[2])/(math.sqrt(sixsum[0]*sixsum[3]-sixsum[1]*sixsum[1]) \
        * math.sqrt(sixsum[0]*sixsum[4]-sixsum[2]*sixsum[2])))

def map_six_val(kv):
    x, y = kv
    return ("r", (1,y[0],y[1],y[0]*y[0],y[1]*y[1],y[0]*y[1]))

def map_host(line):
    return (line['hostname'],(1, line['size']))

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))

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
    nasa_host_count = nasa_table.rdd.map(map_host).reduceByKey(add_tuples).map(map_six_val)
    sum_tuple = nasa_host_count.reduceByKey(add_tuples)
    r = sum_tuple.map(corr_func)
    r2 = r.map(r2_reduce)
    print(r.collect())
    print(r2.collect())


if __name__ == '__main__':
    conf = SparkConf().setAppName('nasa-log-ingest')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    linere = re.compile("^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$")
    inputs = sys.argv[1]
    main(inputs)