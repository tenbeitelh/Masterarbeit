package de.hs.osnabrueck.tenbeitel.mr.extendgraph.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.DateVectorWritable;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.TextPair;

public class DateVectorReducer extends Reducer<TextPair, DateVectorWritable, IntWritable, DateVectorWritable> {

	@Override
	protected void reduce(TextPair key, Iterable<DateVectorWritable> value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int clusterId = Integer.valueOf(key.getSecond().toString());
		DateVectorWritable result;

		Iterator<DateVectorWritable> it = value.iterator();
		DateVectorWritable vectorData = it.next();
		if (it.hasNext()) {
			DateVectorWritable dateData = it.next();
			result = mergeVectors(vectorData, dateData);
			context.write(new IntWritable(clusterId), result);
		} else {
			System.out.println("Received only one vector ==> could not merge");
		}
	}

	private DateVectorWritable mergeVectors(DateVectorWritable vectorData, DateVectorWritable dateData) {

		DateVectorWritable result = new DateVectorWritable();

		if (vectorData.getDate().equals(new Text())) {
			result.setDate(dateData.getDate());
			result.setNamedVector(vectorData.getNamedVector());
		} else {
			result.setDate(vectorData.getDate());
			result.setNamedVector(dateData.getNamedVector());
		}
		return result;
	}

}
