package de.hs.osnabrueck.tenbeitel.mr.twittergraph.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.hs.osnabrueck.tenbeitel.mr.twittergraph.io.TwitterWritable;

public class TwitterDataJoinMapper extends Mapper<LongWritable, Text, Text, TwitterWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TwitterWritable>.Context context)
			throws IOException, InterruptedException {
		String[] textParts = value.toString().split("\t");
		if(textParts.length == 4){
			
		}
		else{
			System.out.println("Not a valid row: " + value.toString());
		}
	}
	
	

}
