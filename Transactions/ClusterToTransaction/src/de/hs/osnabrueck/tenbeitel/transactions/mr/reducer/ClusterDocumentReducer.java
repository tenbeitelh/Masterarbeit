package de.hs.osnabrueck.tenbeitel.transactions.mr.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.common.StringTuple;

import de.hs.osnabrueck.tenbeitel.transactions.mr.io.WritableWrapper;

public class ClusterDocumentReducer extends Reducer<Text, WritableWrapper, Text, StringTuple> {
	MultipleOutputs<Text, StringTuple> mout;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mout = new MultipleOutputs<Text, StringTuple>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<WritableWrapper> values, Context context)
			throws IOException, InterruptedException {

		IntWritable clusterId = null;
		StringTuple tokens = null;
		for (WritableWrapper value : values) {
			if (value.getTokens().getEntries().size() > 0) {
				tokens = new StringTuple(value.getTokens().getEntries());
			} else {
				clusterId = new IntWritable(value.getClusterId().get());
			}
		}

		String dir = clusterId.get() + "/" + clusterId.get();
		System.out.println(key.toString() + " - " + clusterId.get() + " - " + tokens.toString());
		mout.write(new Text(key), tokens, dir);

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mout.close();
	}

}
