package de.hs.osnabrueck.tenbeitel.transactions.mr.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.StringTuple;

public class WritableWrapper implements Writable {
	IntWritable clusterId;
	StringTuple tokens;

	public WritableWrapper() {
		super();
		this.clusterId = new IntWritable();
		this.tokens = new StringTuple();
	}

	public WritableWrapper(IntWritable clusterId, StringTuple tokens) {
		super();
		this.clusterId = clusterId;
		this.tokens = tokens;
	}

	public WritableWrapper(Integer clusterId, Iterable<String> tokens) {
		super();
		this.clusterId = new IntWritable(clusterId);
		this.tokens = new StringTuple(tokens);
	}

	public IntWritable getClusterId() {
		return clusterId;
	}

	public void setClusterId(IntWritable clusterId) {
		this.clusterId = clusterId;
	}

	public void setClusterId(int clusterId) {
		this.clusterId = new IntWritable(clusterId);
	}

	public StringTuple getTokens() {
		return tokens;
	}

	public void setTokens(StringTuple tokens) {
		this.tokens = tokens;
	}

	public void setTokens(Iterable<String> tokens) {
		this.tokens = new StringTuple(tokens);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		clusterId.write(out);
		tokens.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		clusterId.readFields(in);
		tokens.readFields(in);
	}

}
