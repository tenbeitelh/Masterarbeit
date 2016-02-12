package de.hs.osnabrueck.tenbeitel.transactions.mr.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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

		IntWritable clusterId = new IntWritable();
		StringTuple tokens = null;

		Iterator<WritableWrapper> it = values.iterator();
		WritableWrapper temp = it.next();
		WritableWrapper first = new WritableWrapper(temp.getClusterId().get(), temp.getTokens().getEntries());
		if (it.hasNext()) {
			temp = it.next();
			WritableWrapper second = new WritableWrapper(temp.getClusterId().get(), temp.getTokens().getEntries());
			if (first.getTokens().getEntries().size() > 0 && second.getClusterId().get() != 0) {
				clusterId.set(second.getClusterId().get());
				tokens = new StringTuple(first.getTokens().getEntries());
			} else {
				clusterId.set(first.getClusterId().get());
				tokens = new StringTuple(second.getTokens().getEntries());
			}
			String dir = clusterId.get() + "/part";
			System.out.println(key.toString() + " - " + clusterId.get() + " - " + tokens.toString());
			mout.write(new Text(key), tokens, dir);
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mout.close();
	}

}
