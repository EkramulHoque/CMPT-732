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
    new_frame = weather.filter(weather.qflag.isNull()).filter(weather.station.startswith('CA'))
    data_max = new_frame.filter(new_frame.observation == 'TMAX').alias('max')
    data_min = new_frame.filter(new_frame.observation == 'TMIN').alias('min')
    
    #data = new_frame.withColumn("range", (data_max.max - data_min.min) / 10)
    etl_data = data[['station','date','range']]
    # new_frame = data.filter(data.lang == 'en').filter(data.title != 'Main_Page').filter(data.title.contains('Special:') == False).cache()
    # maxs = new_frame.groupBy(new_frame.hour).agg(f.max(new_frame.views).alias('mx')).alias('maxs')
    # result=new_frame.join(maxs,(maxs.hour == new_frame.hour) & (new_frame.views == maxs.mx)).select(new_frame.hour, new_frame.title, new_frame.views).drop(maxs.mx).sort(new_frame.hour, new_frame.title).drop_duplicates()
    # result.write.json(output, mode='overwrite')
    etl_data.show()
   

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

