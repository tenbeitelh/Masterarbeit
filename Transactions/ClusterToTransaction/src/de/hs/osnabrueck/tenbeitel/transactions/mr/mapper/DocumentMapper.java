package de.hs.osnabrueck.tenbeitel.transactions.mr.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.StringTuple;

public class DocumentMapper extends Mapper<Text, StringTuple, Text, Writable> {

	@Override
	protected void map(Text key, StringTuple value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}

}
