package de.hs.osnabrueck.tenbeitel.mr.extendgraph.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.ClusterDateVectorWritable;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.DateVectorWritable;

public class DateVectorReducer extends Reducer<Text, ClusterDateVectorWritable, IntWritable, DateVectorWritable> {

	@Override
	protected void reduce(Text key, Iterable<ClusterDateVectorWritable> value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		ClusterDateVectorWritable result;

		Iterator<ClusterDateVectorWritable> it = value.iterator();
		final ClusterDateVectorWritable vectorData = new ClusterDateVectorWritable(it.next());
		// System.out.println(key.toString());
		// System.out.println(vectorData.toString());
		while (it.hasNext()) {
			final ClusterDateVectorWritable dateData = new ClusterDateVectorWritable(it.next());
			result = mergeVectors(vectorData, dateData);
			System.out.println(vectorData.toString());
			System.out.println(dateData.toString());

			int clusterId = Integer.valueOf(result.getClusterId().toString());
			context.write(new IntWritable(clusterId), result.toDateVectorWritable());

			System.out.println(result.toString());
		}

	}

	private ClusterDateVectorWritable mergeVectors(ClusterDateVectorWritable vectorData,
			ClusterDateVectorWritable dateData) {

		ClusterDateVectorWritable result = new ClusterDateVectorWritable();

		// System.out.println(vectorData.getClusterId() + " " +
		// dateData.getClusterId());
		// System.out.println(vectorData.getDate() + " " + dateData.getDate());

		if (vectorData.getDate().equals(new Text("empty")) && dateData.getClusterId().equals(new Text("N/A"))) {
			result.setDate(dateData.getDate());
			result.setNamedVector(vectorData.getNamedVector());
			result.setClusterId(vectorData.getClusterId());
		} else if (dateData.getDate().equals(new Text("empty")) && vectorData.getClusterId().equals(new Text("N/A"))) {
			result.setDate(vectorData.getDate());
			result.setNamedVector(dateData.getNamedVector());
			result.setClusterId(dateData.getClusterId());
		} else {
			// result = null;
			System.out.println("Merge not possible");
		}
		return result;
	}

}
