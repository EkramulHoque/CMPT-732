import sys
import datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType())
])


def test_model(model_file):

    # load the model
    model = PipelineModel.load(model_file)
    #taken the value of the temperature from the internet 
    inputs = [
    ('sfu',datetime.date(2018, 11, 12),49.2771, -122.9146, 330.0,10.0),
    ('sfu',datetime.date(2018, 11, 11),49.2771, -122.9146, 330.0,11.0)
    ]
    test = spark.createDataFrame(inputs,tmax_schema)
    predictions = model.transform(test)
    print(predictions.select('prediction').collect())

if __name__ == '__main__':
    model_file = sys.argv[1]
    test_model(model_file)
