import sys
from pyspark import SparkContext

if __name__ == "__main__":
 if len(sys.argv) < 2:
  print >> sys.stderr, "Usage: WordCount <file>"
  exit(-1)

 sc=SparkContext()
 mydata=sc.textFile(sys.argv[1])
 m1=mydata.map(lambda k: k.split(":")[8].split(",")[0]+","+k.split(":")[9].split(",")[0])
 

 
 m1.saveAsTextFile(“yelp666”)
 sc.stop()
 #After executing this program in spark, we do some examination and emit a little misordered data points
