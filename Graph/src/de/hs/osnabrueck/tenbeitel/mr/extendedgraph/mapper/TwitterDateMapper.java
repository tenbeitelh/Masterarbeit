package de.hs.osnabrueck.tenbeitel.mr.extendedgraph.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;

import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.DateVectorWritable;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.TextPair;

public class TwitterDateMapper extends Mapper<Text, Text, TextPair, DateVectorWritable> {
	private TextPair keyPair;
	private DateVectorWritable dateVector;

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		keyPair = new TextPair(key, new Text(String.valueOf(Integer.MAX_VALUE)));
		dateVector = new DateVectorWritable(value, new WeightedPropertyVectorWritable());

		context.write(keyPair, dateVector);
	}

}
