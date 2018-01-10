

from __future__ import print_function
import sys
from math import sqrt
import numpy as np
from math import radians, cos, sin, asin, sqrt
from pyspark import SparkContext
import random as rand

# return the index within centroids that is the closest point to p
def closestPoint(p1, centers, type1):
	index=0
	if type1 ==0:
		mindis = EuclideanDistance(p1,centers[0])
	else:
		mindis = haversine(p1,centers[0])
	for i in range(len(centers)):
		if type1 ==0:
			if(EuclideanDistance(p1,centers[i])<mindis):
				mindis = EuclideanDistance(p1,centers[i])
				index = i
		else:
			if(haversine(p1,centers[i])<mindis):
				mindis = haversine(p1,centers[i])
				index = i
	return index



# return a point that is the sum of the two points
def addPoints(p1, p2):
	return p1 + p2


# return the Euclidean distance of two points
def EuclideanDistance(p1, p2):
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

# return the Great Circle Distance of two points
def GreatCircleDistance(p1, p2):
	return haversine(p1, p2)


def parsePoint(line):
	pairs = line.split(",") 
	return np.array([float(x) for x in pairs])

def Covergent(previous,now):

	#print len(previous)
	#print len(now)
	de=0 # original different is 0
	for i in range(len(previous)):
		de = de+EuclideanDistance(previous[i],now[i])
	final = float(de/len(previous))
	return  final


if __name__ == "__main__":
	# handles command line input arguments
	if len(sys.argv) != 4:
		print("Usage: kmeans.py [inputURL] [k] [mode]")
		exit(-1)
	sc = SparkContext()
	# sys.argv[1] passes in the directory that stores our preprocessed data
	lines = sc.textFile(sys.argv[1])
	k = int(sys.argv[2])
	mode = int(sys.argv[3])
	data = lines.map(lambda line: parsePoint(line)).cache()
	centroids = data.takeSample(False, k, 1)
	convergeDist = 1.0
	while convergeDist > 0.1:
		closest = data.map(lambda p: (closestPoint(p, centroids, mode), (p, 1)))
		pointStats = closest.reduceByKey(lambda v1, v2: (addPoints(v1[0], v2[0]), v1[1] + v2[1]))
		updated = pointStats.map(lambda s: (s[1][0] / s[1][1])).collect()
		convergeDist = Covergent(centroids,updated)
		centroids = updated
	#print(closest)
	# write result clusters and centroids to file
	print("Final centroids are: " + str(centroids))
	clusters = data.map(lambda p: (closestPoint(p, centroids, mode), p)).sortByKey()
	
	# savefile, URL can be changed due to different tasks
	clusters.saveAsTextFile("file:///home/training/training_materials/data/k_m_BIG")
	sc.stop()
