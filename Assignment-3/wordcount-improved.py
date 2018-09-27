from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

def main(inputs, output):
	text = sc.textFile(inputs)
	repart = text.repartition(8)
	words = repart.flatMap(words_once).filter(check_length)
	wordcount = words.reduceByKey(operator.add)
	outdata = wordcount.sortBy(get_key).map(output_format)
	outdata.saveAsTextFile(output)

def words_once(line):
	split_words = word_sep.split(line)
	for w in split_words:
		w = w.lower()
		yield (w, 1)

def get_key(kv):
	return kv[0]

def output_format(kv):
	k, v = kv
	return '%s %i' % (k, v)

def check_length(x):
	return len(x) > 0	


if __name__ == '__main__':
	conf = SparkConf().setAppName('word count')
	sc = SparkContext(conf=conf)
	assert sc.version >= '2.3'  # make sure we have Spark 2.3+
	word_sep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
	inputs = sys.argv[1]
	output = sys.argv[2]
	main(inputs, output)