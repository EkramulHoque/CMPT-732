from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

def main(inputs, output):
	text = sc.textFile(inputs)
	json_values = text.map(map_json).filter(filter_word).cache()
	json_values.filter(pos).map(json.dumps).saveAsTextFile(output + '/positive')
	json_values.filter(neg).map(json.dumps).saveAsTextFile(output + '/negative')

def map_json(val):
	data = json.loads(val)
	return (data['subreddit'], data['score'], data['author'])

def filter_word(val):
	return 'e' in val[0]

def pos(val):
	return val[1] > 0	

def neg(val):
	return val[1] <= 0

if __name__ == '__main__':
	conf = SparkConf().setAppName('Reddit Etl')
	sc = SparkContext(conf=conf)
	assert sc.version >= '2.3'  # make sure we have Spark 2.3+
	inputs = sys.argv[1]
	output = sys.argv[2]
	main(inputs, output)