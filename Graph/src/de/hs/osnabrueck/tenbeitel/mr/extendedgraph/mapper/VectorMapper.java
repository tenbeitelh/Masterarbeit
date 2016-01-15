package de.hs.osnabrueck.tenbeitel.mr.extendedgraph.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;

import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.DateVectorWritable;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.TextPair;

public class VectorMapper extends Mapper<IntWritable, WeightedPropertyVectorWritable, TextPair, DateVectorWritable> {

	@Override
	protected void map(IntWritable key, WeightedPropertyVectorWritable value, Context context)
					throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.map(key, value, context);
		
	}
	
	
}
