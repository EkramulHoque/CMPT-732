import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions as f, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+


wiki_schema = types.StructType([ # commented-out fields won't be read
    types.StructField('lang', types.StringType(), True),
    types.StructField('key', types.StringType(), True),
    types.StructField('hit', types.LongType(), True),
    types.StructField('size', types.LongType(), True)
])


def main(inputs, output):
    data = spark.read.csv(inputs, schema=wiki_schema, sep = ' ')
    new_frame = weather.filter(weather.qflag.isNull()).filter(weather.station.startswith('CA')).filter(weather.observation == 'TMAX')
	# cnt = comments.groupBy(comments.subreddit).agg(f.count(comments.subreddit).alias('count'),f.sum(comments.score).alias('sum'))
	# avg = cnt.withColumn('average', cnt['sum'] / cnt['count'] )
	# result = avg[['subreddit','average']]
	# result.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

