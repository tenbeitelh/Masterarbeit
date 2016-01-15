package de.hs.osnabrueck.tenbeitel.mr.extendgraph.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.DateVectorWritable;

public class CalculateDistanceReducer extends Reducer<IntWritable, DateVectorWritable, Text, Text> {

	@Override
	protected void reduce(IntWritable key, Iterable<DateVectorWritable> values, Context context)
			throws IOException, InterruptedException {
		Iterator it = values.iterator();
		
	}

}
