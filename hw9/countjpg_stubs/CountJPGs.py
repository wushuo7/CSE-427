import sys
from pyspark import SparkContext

if __name__ == "__main__":
	if len(sys.argv)<2:
		print >> sys.stderr, "usage: wordCount<file>"
		exit(-1)

	sc = SparkContext()
	counts = sc.textFile(sys.argv[1]).filter(lambda k:  'jpg' in k)
	countsFinal = counts.count()
	print countsFinal
	sc.stop()
