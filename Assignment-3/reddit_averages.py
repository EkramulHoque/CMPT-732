from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

def main(inputs, output):
	text = sc.textFile(inputs)
	json_values = text.map(map_json)
	json_values = json_values.reduceByKey(add_pairs)
	reddit_output = json_values.map(tab_separated)
	reddit_output.saveAsTextFile(output)	
	
def map_json(val):
	data = json.loads(val)
	key = data['subreddit']
	score = data ['score']
	return tuple((key,(1,score)))

def  add_pairs(val1,val2):
	return ((float(val1[0]+val2[0]),float(val1[1]+val2[1])))

def tab_separated(kv):
	average = kv[1][1] / kv[1][0]
	return json.dumps({kv[0] : average})
 

if __name__ == '__main__':
	conf = SparkConf().setAppName('Reddit Average')
	sc = SparkContext(conf=conf)
	assert sc.version >= '2.3'  # make sure we have Spark 2.3+
	inputs = sys.argv[1]
	output = sys.argv[2]
	main(inputs, output)