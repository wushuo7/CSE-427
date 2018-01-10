

import sys
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: wordcount <file>"
        exit(-1)
    sc = SparkContext(appName="project3_step1")
    #mydata=sc.textFile("file:/home/training/training_materials/dev1/data/devicestatus_small.txt")
    mydata=sc.textFile(sys.argv[1])
    d1=mydata.filter(lambda k: ',' in k)
    d2=mydata.filter(lambda k: '/' in k)
    d3=mydata.filter(lambda k: '|' in k)
    d1f=d1.filter(lambda k: len(k.split(","))==14)
    d2f=d2.filter(lambda k: len(k.split("/"))==14)
    d3f=d3.filter(lambda k: len(k.split("|"))==14)
    ex1=d1f.map(lambda k: k.split(",")[12]+"|"+k.split(",")[13]+"|"+k.split(",")[0]+"|"+k.split(",")[1]+"|"+k.split(",")[2])
    ex2=d2f.map(lambda k: k.split("/")[12]+"|"+k.split("/")[13]+"|"+k.split("/")[0]+"|"+k.split("/")[1]+"|"+k.split("/")[2])
    ex3=d3f.map(lambda k: k.split("|")[12]+"|"+k.split("|")[13]+"|"+k.split("|")[0]+"|"+k.split("|")[1]+"|"+k.split("|")[2])
    ex=ex1.union(ex2)
    ex=ex.union(ex3)
    fo=ex.filter(lambda k:(k.split("|")[0]!='0')and(k.split("|")[1]!='0'))
    case1=fo.filter(lambda k: len(k.split(" "))==2)
    case2=fo.filter(lambda k: len(k.split(" "))==4)
    model1=case1.map(lambda k:k.split(" ")[0]+"|"+k.split(" ")[1])
    model2=case2.map(lambda k:k.split(" ")[0]+"|"+k.split(" ")[1]+" "+k.split(" ")[2]+" "+k.split(" ")[3])
    model=model1.union(model2)
    comma=model.map(lambda k:k.split("|")[0]+","+k.split("|")[1]+","+k.split("|")[2]+",manufacturer "+k.split("|")[3]+",model "+k.split("|")[4]+","+k.split("|")[5])
    #comma.saveAsTextFile("file:/home/training/milestone2_test")
    comma.saveAsTextFile("/loudacre/devicestatus_etl")
    sc.stop()

