package de.hs.osnabrueck.tenbeitel.mr.association.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.StringTuple;

import de.hs.osnabrueck.tenbeitel.mr.association.io.ItemSetWritable;

public class AprioriReducer extends Reducer<StringTuple, IntWritable, StringTuple, IntWritable> {
	private static Double minSupport = 1000.0;
	IntWritable support = new IntWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		minSupport = context.getConfiguration().getDouble("apriori.min_support", minSupport);
	}

	@Override
	protected void reduce(StringTuple key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}

		// Write frequent itemsets
		System.out.println(key.toString() + " - Sum: " + sum + " MinSupport: " + minSupport);
		if (sum > minSupport) {
			support.set(sum);
			context.write(key, support);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

}
