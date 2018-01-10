import sys
from pyspark import SparkContext

if __name__ == "__main__":
 if len(sys.argv) < 2:
  print >> sys.stderr, "Usage: WordCount <file>"
  exit(-1)

 #before executing this program, we deleted the summary part in our .csv file first for convenience
 sc=SparkContext()
 mydata=sc.textFile(sys.argv[1])
 m1=mydata.map(lambda k: k.split(",")[5].+","+k.split(",")[6])
 

 
 m1.saveAsTextFile(“UFO666”)
 sc.stop()
 
