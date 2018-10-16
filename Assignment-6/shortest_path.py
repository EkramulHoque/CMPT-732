from pyspark import SparkConf, SparkContext
import sys
import math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

schema = StructType([
    StructField('source', StringType(), False),
    StructField('destination', StringType(), False)
])
schema2 = StructType([
    StructField('node', StringType(), False),
    StructField('source', StringType(), False),
    StructField('distance', IntegerType(), False)
])
schema3 = StructType([
    StructField('node', StringType(), False)
])

def f(x):
    return x

def make_known_path(kv):
    k,v = kv
    return (k,(v.split()))

def word_split(line):
    split_words = line.split(':')
    return split_words

def filter_len(line):
    return len(line) > 1

def main(inputs,output,source,destination):
    text = sc.textFile(inputs)
    #split the line for nodes and its adjacent neighbours and create a pair with each pair ex [1,(3,5)] => [(1,3),(1,5)]
    graph_edges = text.map(word_split).filter(filter_len).map(make_known_path).flatMapValues(f)
    #create the table as (source,destination)
    graph_edges = sqlContext.createDataFrame(graph_edges, schema).cache()
    #create the table as (node,(source,distance))
    known_paths = sc.parallelize([[source,'No Source', 0]])
    known_paths = sqlContext.createDataFrame(known_paths,schema2).cache()
    next_node = sc.parallelize([[source]])
    next_node = sqlContext.createDataFrame(next_node,schema3).cache()
    #get the neighbours of a single node
    for i in range(6):
        neighbours = graph_edges.join(next_node, graph_edges.source == next_node.node).drop('node').cache()
        new_paths = known_paths.join(neighbours, known_paths.node == neighbours.source).select(neighbours.destination,neighbours.source,known_paths.distance)
        new_paths = new_paths.withColumn('new_dist', new_paths.distance + 1).drop(new_paths.distance).cache()
        # union new paths and known paths
        known_paths = known_paths.unionAll(new_paths).dropDuplicates().cache()

        next_node = neighbours.select('source').withColumnRenamed('source','node')
        print("=================")
        known_paths.show()



if __name__ == '__main__':
    conf = SparkConf().setAppName('nasa-log-ingest')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    inputs = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    main(inputs,output,source,destination)