package de.hs.osnabrueck.tenbeitel.mahout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LDAJob extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new LDAJob(), args);
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = this.getConf();
		
		String inputPath = args[0];
		
		
		return 0;
	}

}
