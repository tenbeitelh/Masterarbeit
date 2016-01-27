package de.hs.osnabrueck.tenbeitel.mr.association.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.StringTuple;

public class DetermineTransactionNumberMapper extends Mapper <Text, StringTuple, NullWritable, IntWritable>{
	IntWritable count = new IntWritable(1);
	
	@Override
	protected void map(Text key, StringTuple value,
			Mapper<Text, StringTuple, NullWritable, IntWritable>.Context context)
					throws IOException, InterruptedException {
		context.write(NullWritable.get(), count);
	}
	
	
}
