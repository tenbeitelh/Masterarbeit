package de.hs.osnabrueck.tenbeitel.mr.association.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.StringTuple;

public class KRuleMapper extends Mapper<StringTuple, IntWritable, StringTuple, IntWritable> {

	@Override
	protected void map(StringTuple key, IntWritable value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}

}
