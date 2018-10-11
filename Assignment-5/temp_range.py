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
    free = weather.filter(weather.qflag.isNull()).filter(weather.station.startswith('CA')).cache()

    data_max = weather.filter(weather.qflag.isNull()).filter(weather.observation == 'TMAX').cache()
    data_min = weather.filter(weather.qflag.isNull()).filter(weather.observation == 'TMIN').cache()

    maxs = data_max.groupBy(data_max.date).agg(f.max(data_max.value).alias('tmax')).alias('maxs').sort(data_max.date)
    mins = data_min.groupBy(data_min.date).agg(f.max(data_min.value).alias('tmin')).alias('mins')

    new_max = data_max.join(maxs, (maxs.date==data_max.date)).select(data_max.date,data_max.station,maxs.tmax).sort(data_max.date)
    new_min = data_min.join(mins,(mins.date==data_min.date)).select(data_min.date,data_min.station,mins.tmin)

    data = new_max.join(new_min,(new_max.date == new_min.date) & (new_max.station == new_min.station)).select(new_max.date,new_max.station,new_max.tmax,new_min.tmin)  
    result = data.withColumn('range', (data.tmax-data.tmin)/10).drop('tmax','tmin').sort(data.date,data.station)
    
        
    result.show(n=10)
    
   

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

