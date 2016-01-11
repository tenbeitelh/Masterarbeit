package de.hs.osnabrueck.tenbeitel.mr.informationflow;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InformationflowReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		System.out.println(key.toString() + "{");
		for (Text val : values) {
			System.out.println(val.toString());
		}
		System.out.println("}");

	}

}
