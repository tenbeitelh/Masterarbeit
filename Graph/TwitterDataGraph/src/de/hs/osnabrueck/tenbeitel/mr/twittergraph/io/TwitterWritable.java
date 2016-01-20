package de.hs.osnabrueck.tenbeitel.mr.twittergraph.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TwitterWritable implements Writable {
	private Text clusterId;
	private Text twitterId;
	private Text createdDate;
	private Text tweetMessage;
	private Text userId;
	private Text userScreenName;

	public TwitterWritable() {
		this.clusterId = new Text();
		this.twitterId = new Text();
		this.createdDate = new Text();
		this.tweetMessage = new Text();
		this.userId = new Text();
		this.userScreenName = new Text();
	}

	public TwitterWritable(String clusterId, String twitterId, String createdDate, String tweetMessage, String userId,
			String userScreenName) {
		super();
		this.clusterId = new Text(clusterId);
		this.twitterId = new Text(twitterId);
		this.createdDate = new Text(createdDate);
		this.tweetMessage = new Text(tweetMessage);
		this.userId = new Text(userId);
		this.userScreenName = new Text(userScreenName);
	}

	public TwitterWritable(Text clusterId, Text twitterId, Text createdDate, Text tweetMessage, Text userId,
			Text userScreenName) {
		super();
		this.clusterId = clusterId;
		this.twitterId = twitterId;
		this.createdDate = createdDate;
		this.tweetMessage = tweetMessage;
		this.userId = userId;
		this.userScreenName = userScreenName;
	}

	public TwitterWritable(TwitterWritable delegate) {
		super();
		this.clusterId = new Text(delegate.getClusterId());
		this.twitterId = new Text(delegate.getTwitterId());
		this.createdDate = new Text(delegate.getCreatedDate());
		this.tweetMessage = new Text(delegate.getTweetMessage());
		this.userId = new Text(delegate.getUserId());
		this.userScreenName = new Text(delegate.getUserScreenName());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.clusterId.write(out);
		this.twitterId.write(out);
		this.createdDate.write(out);
		this.tweetMessage.write(out);
		this.userId.write(out);
		this.userScreenName.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.clusterId.readFields(in);
		this.twitterId.readFields(in);
		this.createdDate.readFields(in);
		this.tweetMessage.readFields(in);
		this.userId.readFields(in);
		this.userScreenName.readFields(in);
	}

	public Text getClusterId() {
		return clusterId;
	}

	public void setClusterId(Text clusterId) {
		this.clusterId = clusterId;
	}

	public void setClusterId(String clusterId) {
		this.clusterId = new Text(clusterId);
	}

	public Text getTwitterId() {
		return twitterId;
	}

	public void setTwitterId(Text twitterId) {
		this.twitterId = twitterId;
	}

	public void setTwitterId(String twitterId) {
		this.twitterId = new Text(twitterId);
	}

	public Text getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Text createdDate) {
		this.createdDate = createdDate;
	}

	public void setCreatedDate(String createdDate) {
		this.createdDate = new Text(createdDate);
	}

	public Text getTweetMessage() {
		return tweetMessage;
	}

	public void setTweetMessage(Text tweetMessage) {
		this.tweetMessage = tweetMessage;
	}

	public void setTweetMessage(String tweetMessage) {
		this.tweetMessage = new Text(tweetMessage);
	}

	public Text getUserId() {
		return userId;
	}

	public void setUserId(Text userId) {
		this.userId = userId;
	}

	public void setUserId(String userId) {
		this.userId = new Text(userId);
	}

	public Text getUserScreenName() {
		return userScreenName;
	}

	public void setUserScreenName(Text userScreenName) {
		this.userScreenName = userScreenName;
	}

	public void setUserScreenName(String userScreenName) {
		this.userScreenName = new Text(userScreenName);
	}

}
