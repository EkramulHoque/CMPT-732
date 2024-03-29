from pyspark import SparkConf, SparkContext
import sys
import random
import operator
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def euler(value):
	random.seed(10)
	total_iterations = 0
	for i in value:
		sum = 0.0
		while sum < 1:
			sum += random.random()
			total_iterations+=1
	return total_iterations

def main(inputs,partition_num):
	samples =sc.range(inputs, numSlices=partition_num).collect()
	result = euler(samples)
	print(result/inputs)

if __name__ == '__main__':
	conf = SparkConf().setAppName('Euler')
	sc = SparkContext(conf=conf)
	assert sc.version >= '2.3'  # make sure we have Spark 2.3+
	inputs = sys.argv[1]
	main(int(inputs),partition_num = 4)