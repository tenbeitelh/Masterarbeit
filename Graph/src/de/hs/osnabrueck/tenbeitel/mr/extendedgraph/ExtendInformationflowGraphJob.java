package de.hs.osnabrueck.tenbeitel.mr.extendedgraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExtendInformationflowGraphJob extends Configured implements Tool {

	private static final String INITIAL_GRAPH_PATH = "";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ExtendInformationflowGraphJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception{
		String inputFolder = args[0];
		
		Configuration conf = this.getConf();
		
		Job calculateDistanceJob = Job.getInstance(conf);
		calculateDistanceJob.setJarByClass(ExtendInformationflowGraphJob.class);
		
		
		
		Job extendInformationflowGrahp = Job.getInstance(conf);
		extendInformationflowGrahp.setJarByClass(ExtendInformationflowGraphJob.class);
		extendInformationflowGrahp.addCacheFile(new Path(inputFolder+ "/"+ INITIAL_GRAPH_PATH).toUri());
		extendInformationflowGrahp.setMapperClass(Mapper.class);

		return 0;
	}

}
