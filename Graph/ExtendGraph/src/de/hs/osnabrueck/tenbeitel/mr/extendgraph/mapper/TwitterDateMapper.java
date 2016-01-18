package de.hs.osnabrueck.tenbeitel.mr.extendgraph.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;

import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.ClusterDateVectorWritable;

public class TwitterDateMapper extends Mapper<Text, Text, Text, ClusterDateVectorWritable> {
	
	private ClusterDateVectorWritable dateVector;

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		

		NamedVector vector = new NamedVector(new SequentialAccessSparseVector(0), "N/A");
		Map<Text, Text> properties = new HashMap<Text, Text>();
		WeightedPropertyVectorWritable writableVector = new WeightedPropertyVectorWritable(0, vector, properties);
		dateVector = new ClusterDateVectorWritable(new Text("N/A"), value, writableVector);

		context.write(key, dateVector);
	}

}
