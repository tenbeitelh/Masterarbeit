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
	private Text user;

	public TwitterWritable() {
	}

	public TwitterWritable(String clusterId, String twitterId, String createdDate, String tweetMessage, String user) {
		super();
		this.clusterId = new Text(clusterId);
		this.twitterId = new Text(twitterId);
		this.createdDate = new Text(createdDate);
		this.tweetMessage = new Text(tweetMessage);
		this.user = new Text(user);
	}

	public TwitterWritable(Text clusterId, Text twitterId, Text createdDate, Text tweetMessage, Text user) {
		super();
		this.clusterId = clusterId;
		this.twitterId = twitterId;
		this.createdDate = createdDate;
		this.tweetMessage = tweetMessage;
		this.user = user;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.clusterId.write(out);
		this.twitterId.write(out);
		this.createdDate.write(out);
		this.tweetMessage.write(out);
		this.user.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.clusterId.readFields(in);
		this.twitterId.readFields(in);
		this.createdDate.readFields(in);
		this.tweetMessage.readFields(in);
		this.user.readFields(in);
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

	public Text getUser() {
		return user;
	}

	public void setUser(Text user) {
		this.user = user;
	}

	public void setUser(String user) {
		this.user = new Text(user);
	}

}
