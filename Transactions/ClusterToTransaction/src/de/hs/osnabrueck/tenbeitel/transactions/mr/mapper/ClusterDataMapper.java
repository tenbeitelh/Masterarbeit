package de.hs.osnabrueck.tenbeitel.transactions.mr.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;

public class ClusterDataMapper extends Mapper<IntWritable, WeightedPropertyVectorWritable, Text, Writable> {
	private Text twitterId = new Text();

	@Override
	protected void map(IntWritable key, WeightedPropertyVectorWritable value, Context context)
			throws IOException, InterruptedException {
		NamedVector nVector = (NamedVector) ((WeightedPropertyVectorWritable) value).getVector();
		twitterId.set(nVector.getName());
		context.write(twitterId, key);
	}

}
