package de.hs.osnabrueck.tenbeitel.mr.informationflow.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InformationflowMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text statusId = new Text();
	private Text repliedByStatus = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String statusIds[] = value.toString().split("\t");
		if (statusIds.length == 2) {
			statusId.set(statusIds[1]);
			repliedByStatus.set(statusIds[0]);
			context.write(statusId, repliedByStatus);
		}
	}

}
