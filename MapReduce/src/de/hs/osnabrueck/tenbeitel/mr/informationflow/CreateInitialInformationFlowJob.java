package de.hs.osnabrueck.tenbeitel.mr.informationflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CreateInitialInformationFlowJob extends Configured implements Tool {
	private final static String JOB_NAME = CreateInitialInformationFlowJob.class.getName();

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CreateInitialInformationFlowJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		Job informationflowJob = Job.getInstance(conf);
		informationflowJob.setJobName(JOB_NAME);

		informationflowJob.setJarByClass(CreateInitialInformationFlowJob.class);

		informationflowJob.setMapperClass(InformationflowMapper.class);
		informationflowJob.setCombinerClass(InformationflowCombiner.class);
		informationflowJob.setReducerClass(InformationflowReducer.class);

		informationflowJob.setMapOutputKeyClass(Text.class);
		informationflowJob.setMapOutputValueClass(Text.class);

		informationflowJob.setOutputKeyClass(NullWritable.class);
		informationflowJob.setOutputValueClass(Text.class);

		FileInputFormat.setInputDirRecursive(informationflowJob, true);
		FileInputFormat.addInputPath(informationflowJob, new Path(args[0]));
		informationflowJob.setInputFormatClass(TextInputFormat.class);

		Path outputDir = new Path(args[1]);

		FileSystem fs = FileSystem.get(conf);
		fs.delete(outputDir, true);

		FileOutputFormat.setOutputPath(informationflowJob, outputDir);
		informationflowJob.setOutputFormatClass(TextOutputFormat.class);

		return informationflowJob.waitForCompletion(true) ? 0 : 1;
	}

}
