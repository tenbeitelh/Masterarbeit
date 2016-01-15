package de.hs.osnabrueck.tenbeitel.mr.extendedgraph.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;

import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.DateVectorWritable;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.TextPair;

public class VectorMapper extends Mapper<IntWritable, WeightedPropertyVectorWritable, TextPair, DateVectorWritable> {
	private TextPair keyPair;
	private DateVectorWritable dateVector;

	@Override
	protected void map(IntWritable key, WeightedPropertyVectorWritable value, Context context)
			throws IOException, InterruptedException {
		NamedVector nVector = (NamedVector) ((WeightedPropertyVectorWritable) value).getVector();
		keyPair = new TextPair(nVector.getName(), key.toString());
		dateVector = new DateVectorWritable(new Text(), value);

		context.write(keyPair, dateVector);

	}

}
