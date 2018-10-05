import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('Weather Etl').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary
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
    weather = spark.read.csv(inputs, schema=observation_schema)
    new_frame = weather.filter(weather.qflag.isNull()).filter(weather.station.startswith('CA')).filter(weather.observation == 'TMAX')
    data = new_frame.withColumn("tmax", weather.value / 10)
    etl_data = data[['station','date','tmax']]
    etl_data.write.json(output, compression='gzip', mode='overwrite')
    #etl_data.show(n=10)
    
    # main logic starts here

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)