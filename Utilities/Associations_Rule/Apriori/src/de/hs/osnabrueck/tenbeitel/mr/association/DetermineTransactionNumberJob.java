package de.hs.osnabrueck.tenbeitel.mr.association;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import de.hs.osnabrueck.hadoop.util.HadoopPathUtils;
import de.hs.osnabrueck.tenbeitel.mr.association.mapper.DetermineTransactionNumberMapper;
import de.hs.osnabrueck.tenbeitel.mr.association.reducer.DetermineTransactionNumberReducer;

public class DetermineTransactionNumberJob extends Configured implements Tool {

	private static final String JOB_NAME = "DetermineTransactionNumberJob";

	@Override
	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			System.out.println("No input and output path");
			return 1;
		}
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Configuration conf = this.getConf();

		return run(conf, inputPath, outputPath);
	}

	public int run(Configuration conf, Path inputPath, Path outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job determineTransactionNumber = Job.getInstance(conf);
		determineTransactionNumber.setJobName(JOB_NAME);
		determineTransactionNumber.setJarByClass(DetermineTransactionNumberJob.class);

		determineTransactionNumber.setOutputKeyClass(NullWritable.class);
		determineTransactionNumber.setOutputValueClass(IntWritable.class);

		determineTransactionNumber.setMapperClass(DetermineTransactionNumberMapper.class);
		determineTransactionNumber.setReducerClass(DetermineTransactionNumberReducer.class);

		determineTransactionNumber.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.setInputDirRecursive(determineTransactionNumber, true);
		FileInputFormat.addInputPath(determineTransactionNumber, inputPath);

		HadoopPathUtils.deletePathIfExists(conf, outputPath);

		determineTransactionNumber.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(determineTransactionNumber, outputPath);

		return determineTransactionNumber.waitForCompletion(true) ? 0 : 1;
	}

}
