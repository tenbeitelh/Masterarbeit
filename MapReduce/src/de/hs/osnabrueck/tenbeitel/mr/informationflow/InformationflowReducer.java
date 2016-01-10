package de.hs.osnabrueck.tenbeitel.mr.informationflow;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InformationflowReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder vals = new StringBuilder();
		for (Text val : values) {
			vals.append(val);
			vals.append(" ");
		}
		System.out.println(key.toString() + " -> { " + values.toString() + "}");

	}

}
