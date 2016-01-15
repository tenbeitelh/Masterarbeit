package de.hs.osnabrueck.tenbeitel.mr.extendedgraph.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;

import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.ClusterDateVectorWritable;

public class VectorMapper extends Mapper<IntWritable, WeightedPropertyVectorWritable, Text, ClusterDateVectorWritable> {
	private Text keyText;
	private ClusterDateVectorWritable dateVector;

	@Override
	protected void map(IntWritable key, WeightedPropertyVectorWritable value, Context context)
			throws IOException, InterruptedException {
		NamedVector nVector = (NamedVector) ((WeightedPropertyVectorWritable) value).getVector();
		keyText = new Text(nVector.getName());
		dateVector = new ClusterDateVectorWritable(new Text(key.toString()), new Text("empty"), value);

		context.write(keyText, dateVector);

	}

}
