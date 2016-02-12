package de.hs.osnabrueck.tenbeitel.transactions.mr.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.StringTuple;

import de.hs.osnabrueck.tenbeitel.transactions.mr.io.WritableWrapper;

public class DocumentMapper extends Mapper<Text, StringTuple, Text, WritableWrapper> {
	private WritableWrapper wrapper = new WritableWrapper();

	@Override
	protected void map(Text key, StringTuple value, Context context) throws IOException, InterruptedException {
		wrapper.setTokens(value.getEntries());
		context.write(key, wrapper);
	}

}
