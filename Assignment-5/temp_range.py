import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions as f, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+


observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),
])

def main(inputs, output):
    weather = spark.read.csv(inputs, schema=observation_schema).drop_duplicates()
    data_max = weather.filter(weather.qflag.isNull()).filter(weather.observation == 'TMAX').cache()
    data_min = weather.filter(weather.qflag.isNull()).filter(weather.observation == 'TMIN').cache()
    data_max = data_max.withColumn('tmax',data_max.value)
    data_min = data_min.withColumn('tmin',data_min.value)
    data = data_max.join(data_min,(data_max.date == data_min.date) & (data_max.station == data_min.station)).select(data_max.date,data_max.station,data_max.tmax,data_min.tmin)
    range = data.withColumn('range', (data.tmax-data.tmin)/10).drop('tmax','tmin')
    range_reduce = range.groupBy(data.date).agg(f.max(range.range).alias('range_max'))
    result = range.join(range_reduce,(range_reduce.date==range.date) & (range_reduce.range_max==range.range)).select(range.date,range.station,range.range).sort(range.date)
    #result.show(n=10)
    result.write.json(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

