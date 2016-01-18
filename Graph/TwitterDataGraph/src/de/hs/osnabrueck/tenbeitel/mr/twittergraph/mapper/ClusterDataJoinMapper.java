package de.hs.osnabrueck.tenbeitel.mr.twittergraph.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;

import de.hs.osnabrueck.tenbeitel.mr.twittergraph.io.TwitterWritable;

public class ClusterDataJoinMapper extends Mapper<IntWritable, WeightedPropertyVectorWritable, Text, TwitterWritable>{

}
