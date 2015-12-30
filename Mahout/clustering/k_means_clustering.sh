#!/bin/bash

now="$(date +'%d/%m/%Y')"

if [[ $# < 3 ]]; 
	then
		echo 'No input parameters'
		exit
	else
		inputDir=$1
		outputDir=$2
fi

cluster_output="$outputDir_$now"
initial_centroids="$outputDir\initial_centroids"

mahout kmeans \
--input $inputDir \
--output cluster_output \
--clusters $initial_centroids \
--numClusters 3 \
--distanceMeasure org.apache.mahout.common.distance.CosineDistanceMeasure \
--maxIter 20 \
--method mapreduce