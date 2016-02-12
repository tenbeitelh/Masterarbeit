package de.hs.osnabrueck.tenbeitel.transactions.mr.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;

import de.hs.osnabrueck.tenbeitel.transactions.mr.io.WritableWrapper;

public class ClusterDataMapper extends Mapper<IntWritable, WeightedPropertyVectorWritable, Text, WritableWrapper> {
	private Text twitterId = new Text();

	private WritableWrapper wrapper = new WritableWrapper();

	@Override
	protected void map(IntWritable key, WeightedPropertyVectorWritable value, Context context)
			throws IOException, InterruptedException {
		NamedVector nVector = (NamedVector) ((WeightedPropertyVectorWritable) value).getVector();
		twitterId.set(nVector.getName());
		wrapper.setClusterId(key.get());
		context.write(twitterId, wrapper);
	}

}
