package de.hs.osnabrueck.tenbeitel.mr.twittergraph.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.hs.osnabrueck.tenbeitel.mr.twittergraph.constants.TwitterWritableConstants;
import de.hs.osnabrueck.tenbeitel.mr.twittergraph.io.TwitterWritable;

public class TwitterDataJoinMapper extends Mapper<LongWritable, Text, Text, TwitterWritable> {
	private Text twitterIdKey = new Text();
	private TwitterWritable twitterValue = new TwitterWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TwitterWritable>.Context context)
			throws IOException, InterruptedException {
		String[] textParts = value.toString().split("|");
		if (textParts.length == 5) {
			twitterIdKey.set(textParts[0]);
			twitterValue.setTwitterId(textParts[0]);
			twitterValue.setCreatedDate(textParts[1]);
			twitterValue.setUserId(textParts[2]);
			twitterValue.setUserScreenName(textParts[3]);
			twitterValue.setTweetMessage(textParts[4]);
			twitterValue.setClusterId(TwitterWritableConstants.NOT_AVAILABLE);
			context.write(twitterIdKey, twitterValue);
		} else {
			System.out.println("Not a valid row: " + value.toString());
		}
	}

}
