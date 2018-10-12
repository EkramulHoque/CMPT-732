import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions as f, types, SQLContext

spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+

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
    weather = spark.read.csv(inputs, schema=observation_schema).drop_duplicates().cache()
    weather.createOrReplaceTempView("weather")
    data_max = spark.sql("SELECT date, station , value as tmax FROM weather where qflag is NULL AND observation = 'TMAX' ")
    data_min = spark.sql("SELECT date, station , value as tmin FROM weather where qflag is NULL AND observation = 'TMIN' ")
    data_max.createOrReplaceTempView("data_max")
    data_min.createOrReplaceTempView("data_min")
    range = spark.sql("SELECT data_max.date as date, data_max.station as station, CAST((data_max.tmax - data_min.tmin)/10 AS DOUBLE) as range FROM data_max JOIN data_min ON data_max.date = data_min.date AND data_max.station = data_min.station ORDER BY data_max.date")
    range.createOrReplaceTempView("range")
    range_reduce = spark.sql("Select range.date, MAX(range.range) as range_max from range GROUP BY range.date")
    range_reduce.createOrReplaceTempView("range_reduce")
    result = spark.sql("SELECT range.date as date, range.station as station, range.range FROM range JOIN range_reduce ON range.date = range_reduce.date AND range.range = range_reduce.range_max ORDER BY range.date")
    result.write.json(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

