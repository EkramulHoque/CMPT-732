from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

def main(inputs, output):
	text = sc.textFile(inputs)
	text = text.map(json.loads).cache()
	json_values = text.map(map_json).reduceByKey(add_pairs)
	averages = json_values.map(get_average).filter(filter_average)
	commentbysub = text.map(key_comment).join(averages).map(get_relative_score)
	commentbysub.sortBy(lambda x: x[0], False).saveAsTextFile(output)	
	
def map_json(val):
	key = val['subreddit']
	score = val ['score']
	return tuple((key,(1,score)))

def key_comment(val):
	key = val['subreddit']
	return tuple((key,val))


def add_pairs(val1,val2):
	return (val1[0]+val2[0],val1[1]+val2[1])

def get_average(kv):
	k, v = kv
	return (k, v[1]/v[0])

def filter_average(val):
	return val[1] > 0

def get_relative_score(kv):
	k, v = kv
	return (v[0]['score']/v[1], v[0]['author'])


if __name__ == '__main__':
	conf = SparkConf().setAppName('Relative Average')
	sc = SparkContext(conf=conf)
	assert sc.version >= '2.3'  # make sure we have Spark 2.3+
	inputs = sys.argv[1]
	output = sys.argv[2]
	main(inputs, output)