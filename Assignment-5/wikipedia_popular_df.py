import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions as f, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+


wiki_schema = types.StructType([ # commented-out fields won't be read
     types.StructField('hour', types.StringType(), True),
    types.StructField('lang', types.StringType(), True),
    types.StructField('title', types.StringType(), True),
    types.StructField('views', types.LongType(), True),
    types.StructField('size', types.LongType(), True)
])

def path_to_hour(path):
    result = f.split(path,'/')
    file_name = f.split(result[f.size(result)-1],'pagecounts-')
    return file_name[f.size(file_name)-1].substr(1,11)


def main(inputs, output):
    data = spark.read.csv(inputs, schema=wiki_schema, sep = ' ').withColumn("hour", path_to_hour(f.input_file_name()))
    new_frame = data.filter(data.lang == 'en').filter(data.title != 'Main_Page').filter(data.title.contains('Special:') == False).cache()
    maxs = new_frame.groupBy(new_frame.hour).agg(f.max(new_frame.views).alias('mx')).alias('maxs')
    result=new_frame.join(maxs,(maxs.hour == new_frame.hour) & (new_frame.views == maxs.mx)).select(new_frame.hour, new_frame.title, new_frame.views).drop(maxs.mx).sort(new_frame.hour, new_frame.title).drop_duplicates()
    #result.write.json(output, mode='overwrite')
    result.show(n=100)
   

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

