package de.hs.osnabrueck.tenbeitel.mr.extendedgraph;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.hs.osnabrueck.tenbeitel.mr.extendedgraph.mapper.TwitterDateMapper;
import de.hs.osnabrueck.tenbeitel.mr.extendedgraph.mapper.VectorMapper;
import de.hs.osnabrueck.tenbeitel.mr.extendedgraph.utils.HadoopPathUtils;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.ClusterDateVectorWritable;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.DateVectorWritable;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.reducer.DateVectorReducer;

public class ExtendInformationflowGraphJob extends Configured implements Tool {

	private static final String INITIAL_GRAPH_PATH = "";
	private static final String TWITTER_ID_DATE_FOLDER = "twitter_id_date";
	private static final String CLUSTERED_POINTS_DIR = "kmeans/clusters/clusteredPoints";
	private static final String TEMP_DATE_VECTOR_DIR = "date_vectors";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ExtendInformationflowGraphJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		String inputFolder = args[0];

		Configuration conf = this.getConf();

		if (args.length > 1) {
			conf.set("reducer.treshold", args[1]);
		}

		runBuildDateVectorJob(conf, inputFolder);

		// Job calculateDistanceJob = Job.getInstance(conf);
		// calculateDistanceJob.setJarByClass(ExtendInformationflowGraphJob.class);
		//
		// Job extendInformationflowGrahp = Job.getInstance(conf);
		// extendInformationflowGrahp.setJarByClass(ExtendInformationflowGraphJob.class);
		// extendInformationflowGrahp.addCacheFile(new Path(inputFolder + "/" +
		// INITIAL_GRAPH_PATH).toUri());
		// extendInformationflowGrahp.setMapperClass(Mapper.class);

		return 0;
	}

	private int runBuildDateVectorJob(Configuration conf, String inputFolder)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job buildDateVector = Job.getInstance(conf);
		buildDateVector.setJobName(this.getClass().getName() + " - BuildDateVectorJob");
		buildDateVector.setJarByClass(ExtendInformationflowGraphJob.class);

		Path twitterIdDateMap = new Path(inputFolder, TWITTER_ID_DATE_FOLDER);
		Path clusteredPoints = new Path(inputFolder, CLUSTERED_POINTS_DIR);

		MultipleInputs.addInputPath(buildDateVector, twitterIdDateMap, SequenceFileInputFormat.class,
				TwitterDateMapper.class);
		MultipleInputs.addInputPath(buildDateVector, clusteredPoints, SequenceFileInputFormat.class,
				VectorMapper.class);

		FileInputFormat.setInputDirRecursive(buildDateVector, true);

		buildDateVector.setMapOutputKeyClass(Text.class);
		buildDateVector.setMapOutputValueClass(ClusterDateVectorWritable.class);

		buildDateVector.setReducerClass(DateVectorReducer.class);
		buildDateVector.setOutputKeyClass(IntWritable.class);
		buildDateVector.setOutputValueClass(DateVectorWritable.class);

		buildDateVector.setOutputFormatClass(SequenceFileOutputFormat.class);

		Path outputPath = new Path(inputFolder, TEMP_DATE_VECTOR_DIR);
		HadoopPathUtils.deletePathIfExists(conf, outputPath);
		FileOutputFormat.setOutputPath(buildDateVector, outputPath);

		return (buildDateVector.waitForCompletion(true) ? 0 : 1);

	}

	private int runCalculateDistanceJob(Configuration conf, String inputFolder) throws IOException {
		Job calculateDisanceJob = Job.getInstance(conf);
		calculateDisanceJob.setJobName(this.getClass().getName() + " - CalculateDistanceJob");
		calculateDisanceJob.setJarByClass(ExtendInformationflowGraphJob.class);

		Path dateVectorPath = new Path(inputFolder, TEMP_DATE_VECTOR_DIR);

		calculateDisanceJob.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.setInputDirRecursive(calculateDisanceJob, true);
		FileInputFormat.addInputPath(calculateDisanceJob, dateVectorPath);

		calculateDisanceJob.setMapperClass(Mapper.class);
		calculateDisanceJob.setMapOutputKeyClass(IntWritable.class);
		calculateDisanceJob.setMapOutputValueClass(DateVectorWritable.class);

		calculateDisanceJob.setOutputKeyClass(Text.class);
		calculateDisanceJob.setOutputValueClass(Text.class);

		return 0;
	}

}
