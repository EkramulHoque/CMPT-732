from pyspark import SparkConf, SparkContext
import sys
import random
import operator
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def euler(value):
	random.seed()
	total_iterations = 0
	for i in range(int(value)):
		sum = 0.0
		while sum < 1:
			sum += random.random()
			total_iterations+=1
	return total_iterations

def main(inputs,partition_num):
	samples =sc.parallelize([inputs/4]* 4, numSlices=partition_num).map(euler).reduce(operator.add)
	print(samples/inputs)

if __name__ == '__main__':
	conf = SparkConf().setAppName('Euler')
	sc = SparkContext(conf=conf)
	assert sc.version >= '2.3'  # make sure we have Spark 2.3+
	inputs = sys.argv[1]
	main(int(inputs),partition_num = 4)