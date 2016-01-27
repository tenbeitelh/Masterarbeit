package de.hs.osnabrueck.tenbeitel.mr.association.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import de.hs.osnabrueck.tenbeitel.mr.association.io.ItemSetWritable;

public class AprioriReducer extends Reducer<ItemSetWritable, IntWritable, Writable, Writable> {
	private static Double minSupport = 1000.0;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		minSupport = context.getConfiguration().getDouble("apriori.min_support", minSupport);
	}

	@Override
	protected void reduce(ItemSetWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		
		if (sum > minSupport) {
			
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

}
