package de.hs.osnabrueck.tenbeitel.mr.extendedgraph.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;

import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.DateVectorWritable;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.TextPair;

public class TwitterDateMapper extends Mapper<Text, Text, TextPair, DateVectorWritable> {
	private TextPair keyPair;
	private DateVectorWritable dateVector;

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		keyPair = new TextPair(key, new Text(String.valueOf(Integer.MAX_VALUE)));

		NamedVector vector = new NamedVector(new SequentialAccessSparseVector(0), "N/A");
		Map<Text, Text> properties = new HashMap<Text, Text>();
		WeightedPropertyVectorWritable writableVector = new WeightedPropertyVectorWritable(0, vector, properties);
		dateVector = new DateVectorWritable(value, writableVector);

		context.write(keyPair, dateVector);
	}

}
