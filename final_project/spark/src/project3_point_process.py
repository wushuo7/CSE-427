
from __future__ import print_function
import sys
from operator import add
import random as rand
from pyspark import SparkContext
import math
import numpy as np
from math import radians, cos, sin, asin, sqrt
from array import *
import random as rand

def EucDistance(p1,p2):
	d1=p1[0]-p2[0]
	d2=p1[1]-p2[1]
	d=d1**2+d2**2
	dis = d**0.5
	return dis

def haversine(point1,point2):
	"""
	Calculate the great circle distance between two points
	on the earth (specified in decimal degrees)
	"""
	# convert decimal degrees to radians
	earth = 6378137
	point1[1], point1[0], point2[1], point2[0] = map(radians, [point1[1], point1[0], point2[1], point2[0]])
	# haversine formula
	dlon = point2[1] - point1[1]
	dlat = point2[0] - point1[0]
	a = sin(dlat / 2) ** 2 + cos(point1[0]) * cos(point2[0]) * sin(dlon / 2) ** 2
	c = 2 * asin(sqrt(a))
	# Radius of earth in kilometers is 6371
	gcd = earth * c
	return gcd

def addPoint(p1,p2):
	point = p1+p2
	return point

def closest(p1,centers,type):
	index=0
	if type ==0:
		mindis = EucDistance(p1,centers[0])
	else:
		mindis = haversine(p1,centers[0])
	for i in range(len(centers)):
		if type ==0:
			if(EucDistance(p1,centers[i])<mindis):
				mindis = EucDistance(p1,centers[i])
				index = i
		else:
			if(haversine(p1,centers[i])<mindis):
				mindis = haversine(p1,centers[i])
				index = i
	return index

def Covergent(previous,now):
	de=0.0 # original different is 0
	print len(previous)
	print len(now)
	for i in range(len(previous)):
		de = de+EucDistance(previous[i],now[i])
	final = float(de/len(previous))
	
	return  final

def parseLine(line):
	location = line.split(",") 
	return np.array([float(i) for i in location])

if __name__ == "__main__":
    if len(sys.argv) != 1:
        print >> sys.stderr, "Usage: wordcount <file>"
        exit(-1)
    sc = SparkContext(appName="project3_proess_")
    #k=int(sys.argv[1])
    type1 =0
    mydata=sc.textFile("file:/home/training/Downloads/points/part-00000")
    points = mydata.map(lambda line: parseLine(line)).cache()
    centers = points.takeSample(False,3,1)
    conve_value = float(1.0)

    #mydata=sc.textFile("file:/home/training/Downloads/sample_geo.txt")
    while conve_value > 0.1:
	closest = points.map(lambda p:(closest(p,centers,type1),(p, 1)))
	closest = data.map(lambda p: (closestPoint(p, centroids, mode), (p, 1)))
	cluster_which = closest.reduceByKey(lambda x,y:(addPoint(x[0],y[0]),x[1]+y[1]))
	new_centers = cluster_which.map(lambda v:(v[1][0]/v[1][1])).collect()
	conve_value = Covergent(centers,new_centers)
 	centers = new_centers
	
	
    
    print(centers)
    
    
    
    
    #model1.saveAsTextFile("file:/home/training/Downloads/step21")
    sc.stop()

