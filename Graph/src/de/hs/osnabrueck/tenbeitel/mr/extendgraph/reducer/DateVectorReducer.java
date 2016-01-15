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
		while (it.hasNext()) {
			ClusterDateVectorWritable dateData = it.next();
			result = mergeVectors(vectorData, dateData);
			int clusterId = Integer.valueOf(result.getClusterId().toString());
			context.write(new IntWritable(clusterId), result.toDateVectorWritable());
			System.out.println(clusterId + " - " + result.toString());
		}
		

	}

	private ClusterDateVectorWritable mergeVectors(ClusterDateVectorWritable vectorData,
			ClusterDateVectorWritable dateData) {

		ClusterDateVectorWritable result = new ClusterDateVectorWritable();

		if (vectorData.getDate().equals(new Text("empty"))) {
			result.setDate(dateData.getDate());
			result.setNamedVector(vectorData.getNamedVector());
			result.setClusterId(vectorData.getClusterId());
		} else {
			result.setDate(vectorData.getDate());
			result.setNamedVector(dateData.getNamedVector());
			result.setClusterId(dateData.getClusterId());
		}
		return result;
	}

}
