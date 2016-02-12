package de.hs.osnabrueck.tenbeitel.transactions.mr.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.common.StringTuple;

public class ClusterDocumentReducer extends Reducer<Text, Writable, Text, StringTuple> {
	MultipleOutputs<Text, StringTuple> mout;

	@Override
	protected void setup(Reducer<Text, Writable, Text, StringTuple>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		mout = new MultipleOutputs<Text, StringTuple>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Writable> values, Context context)
			throws IOException, InterruptedException {

		IntWritable clusterId = null;
		StringTuple tokens = null;
		for (Writable value : values) {
			if (value instanceof StringTuple) {
				tokens = new StringTuple(((StringTuple) value).getEntries());
			}
			if (value instanceof IntWritable) {
				clusterId = (IntWritable) value;
			}
		}

		String dir = clusterId.get() + "/" + clusterId.get();
		mout.write(key, tokens, dir);

	}

	@Override
	protected void cleanup(Reducer<Text, Writable, Text, StringTuple>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		mout.close();
	}

}
