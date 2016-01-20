package de.hs.osnabrueck.tenbeitel.mr.twittergraph.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;

import de.hs.osnabrueck.tenbeitel.mr.twittergraph.constants.TwitterWritableConstants;
import de.hs.osnabrueck.tenbeitel.mr.twittergraph.io.TwitterWritable;

public class ClusterDataJoinMapper extends Mapper<IntWritable, WeightedPropertyVectorWritable, Text, TwitterWritable> {

	private Text twitterIdKey = new Text();
	private TwitterWritable twitterValue = new TwitterWritable();

	@Override
	protected void map(IntWritable key, WeightedPropertyVectorWritable value, Context context)
			throws IOException, InterruptedException {
		String clusterId = key.toString();
		NamedVector nVector = (NamedVector) ((WeightedPropertyVectorWritable) value).getVector();
		String twitterId = nVector.getName();
		twitterIdKey.set(twitterId);

		twitterValue.setTwitterId(twitterId);
		twitterValue.setClusterId(clusterId);
		twitterValue.setCreatedDate(TwitterWritableConstants.NOT_AVAILABLE);
		twitterValue.setUserId(TwitterWritableConstants.NOT_AVAILABLE);
		twitterValue.setUserScreenName(TwitterWritableConstants.NOT_AVAILABLE);
		twitterValue.setTweetMessage(TwitterWritableConstants.NOT_AVAILABLE);

		context.write(twitterIdKey, twitterValue);
	}

}
