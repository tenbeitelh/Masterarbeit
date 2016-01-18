package de.hs.osnabrueck.tenbeitel.mr.twittergraph;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TwitterDataGraphJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TwitterDataGraphJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = this.getConf();
		int res = 0;
		
		res += runJoinTwitterAndClusterDataJob(conf);
		
		return 0;
	}

	private int runJoinTwitterAndClusterDataJob(Configuration conf) throws IOException {
		// TODO Auto-generated method stub
		
		Job joinTwitterAndClusterData = Job.getInstance(conf);
		
		return 0;
	}

}
