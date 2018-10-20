from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SQLContext, functions
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
    k, v = kv
    return (k, (v.split()))

def word_split(line):
    split_words = line.split(':')
    return split_words

def filter_len(line):
    return len(line) > 1

def debug_result(x):
    return "node %s: source %s, distance %i" % (x[0], x[1], x[2])

def main(inputs, output, source, destination):
    text = sc.textFile(inputs)
    graph_edges = text.map(word_split).filter(filter_len).map(make_known_path).flatMapValues(f)
    graph_edges = sqlContext.createDataFrame(graph_edges, schema).cache()
    known_paths = sc.parallelize([[source, 'No Source', 0]])
    known_paths = sqlContext.createDataFrame(known_paths, schema2).cache()
    next_node = sc.parallelize([[source]])
    next_node = sqlContext.createDataFrame(next_node, schema3).cache()
    for i in range(6):
        neighbours = graph_edges.join(next_node, graph_edges.source == next_node.node).drop('node').cache()
        new_paths = known_paths.join(neighbours, known_paths.node == neighbours.source).select(neighbours.destination,
                                                                                               neighbours.source,
                                                                                               known_paths.distance)
        new_paths = new_paths.withColumn('new_dist', new_paths.distance + 1).drop(new_paths.distance).cache()
        known_paths = known_paths.unionAll(new_paths).dropDuplicates().cache()
        min_paths = known_paths.groupBy(known_paths.node).agg(
            functions.min(known_paths.distance).alias('min_distance')).withColumnRenamed('node', 'min_node')
        known_paths = min_paths.join(known_paths, (min_paths.min_node == known_paths.node) & (
                    min_paths.min_distance == known_paths.distance)).drop('min_node', 'min_distance').cache()
        next_node = neighbours.select('destination').withColumnRenamed('destination', 'node')
        debug = known_paths.rdd.map(debug_result)
        debug.saveAsTextFile(output + '/iter-' + str(i))
        if next_node[next_node.node == destination].count() > 0:
            break
    known_paths.show()
    outdata = []
    path = destination
    outdata.insert(0, path)
    while (path != source):
        path = known_paths[known_paths.node == path].select('source').first()[0]
        outdata.insert(0, path)
    outdata = sc.parallelize(outdata)
    outdata.saveAsTextFile(output + '/path')

if __name__ == '__main__':
    conf = SparkConf().setAppName('nasa-log-ingest')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    inputs = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    main(inputs, output, source, destination)