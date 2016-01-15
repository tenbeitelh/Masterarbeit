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
		System.out.println(key.toString());
		Iterator<ClusterDateVectorWritable> it = value.iterator();
		ClusterDateVectorWritable vectorData = it.next();
		System.out.println(key.toString());
		System.out.println(vectorData);
		while (it.hasNext()) {
			ClusterDateVectorWritable dateData = it.next();
			result = mergeVectors(vectorData, dateData);
			System.out.println(dateData.toString());
			if (result == null)
				continue;
			// int clusterId =
			// Integer.valueOf(result.getClusterId().toString());
			// context.write(new IntWritable(clusterId),
			// result.toDateVectorWritable());

			System.out.println(result.toString());
		}

	}

	private ClusterDateVectorWritable mergeVectors(ClusterDateVectorWritable vectorData,
			ClusterDateVectorWritable dateData) {

		ClusterDateVectorWritable result = new ClusterDateVectorWritable();

		if (vectorData.getDate().equals(new Text("empty")) && dateData.getClusterId().equals(new Text("N/A"))) {
			result.setDate(dateData.getDate());
			result.setNamedVector(vectorData.getNamedVector());
			result.setClusterId(vectorData.getClusterId());
		} else if (dateData.getDate().equals(new Text("empty")) && vectorData.getClusterId().equals(new Text("N/A"))) {
			result.setDate(vectorData.getDate());
			result.setNamedVector(dateData.getNamedVector());
			result.setClusterId(dateData.getClusterId());
		} else {
			result = null;
			System.out.println("Merge not possible");
		}
		return result;
	}

}
