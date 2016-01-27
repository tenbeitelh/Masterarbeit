package de.hs.osnabrueck.tenbeitel.mr.association.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DetermineTransactionNumberReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
	IntWritable sumTransactions = new IntWritable();

	@Override
	protected void reduce(NullWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		sumTransactions.set(sum);
		context.write(key, sumTransactions);
	}

}
