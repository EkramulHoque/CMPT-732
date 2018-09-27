from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

 

def mySplit(line):
	x = line.split()
	if(len(x)>0):
		time_stamp= x[0]
		x[0] = time_stamp[:11]
		x[3] = int(x[3])
	return tuple(x)
		
def check_tuple_en(lines):
	for line in lines:
		if lines[1].lower() =='en' and 'Main_Page' not in lines[2] and 'Special:' not in lines[2]:
			return lines

def get_key(kv):
	return (kv[0], (kv[3],kv[2]))


def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])


def get_key_1(kv):
	return kv[0]



text = sc.textFile(inputs).map(mySplit).filter(check_tuple_en)
 
filtered = text.map(get_key) 

filtered = filtered.reduceByKey(max)

#print(filtered.collect())

outdata = filtered.sortBy(get_key_1)

outdata = outdata.map(tab_separated)

outdata.saveAsTextFile(output)
#print(outdata.collect())
 
