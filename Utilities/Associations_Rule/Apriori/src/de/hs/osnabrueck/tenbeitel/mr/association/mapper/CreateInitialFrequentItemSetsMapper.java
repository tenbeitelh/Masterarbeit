package de.hs.osnabrueck.tenbeitel.mr.association.mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.StringTuple;

import de.hs.osnabrueck.tenbeitel.mr.association.io.ItemSetWritable;

public class CreateInitialFrequentItemSetsMapper extends Mapper<Text, StringTuple, ItemSetWritable, IntWritable> {
	IntWritable frequencyCount = new IntWritable(1);

	Set<String> recognicedTokens;

	@Override
	protected void map(Text key, StringTuple value, Context context) throws IOException, InterruptedException {

		recognicedTokens = new HashSet<String>();

		for (String token : value.getEntries()) {
			if (recognicedTokens.add(token)) {
				context.write(new ItemSetWritable(new String[] { token }), frequencyCount);
			}
		}
	}

}
