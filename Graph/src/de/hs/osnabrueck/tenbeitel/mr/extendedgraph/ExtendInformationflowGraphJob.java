package de.hs.osnabrueck.tenbeitel.mr.extendedgraph;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.hs.osnabrueck.tenbeitel.mr.extendedgraph.mapper.ExtendGraphMapper;
import de.hs.osnabrueck.tenbeitel.mr.extendedgraph.mapper.TwitterDateMapper;
import de.hs.osnabrueck.tenbeitel.mr.extendedgraph.mapper.VectorMapper;
import de.hs.osnabrueck.tenbeitel.mr.extendedgraph.utils.HadoopPathUtils;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.ClusterDateVectorWritable;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.io.DateVectorWritable;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.reducer.CalculateDistanceReducer;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.reducer.DateVectorReducer;
import de.hs.osnabrueck.tenbeitel.mr.extendgraph.reducer.ExtendGraphReducer;

public class ExtendInformationflowGraphJob extends Configured implements Tool {

	
	private static final String INITIAL_GRAPH_PATH = "initial_informationflow";
	private static final String TWITTER_ID_DATE_FOLDER = "twitter_id_date";
	private static final String CLUSTERED_POINTS_DIR = "kmeans/clusters/clusteredPoints";
	private static final String TEMP_DATE_VECTOR_DIR = "date_vectors";
	private static final String SIMILAR_TWITTER_FOLDER = "similar_twitter_id";
	private static final String EXTENDED_GRAPH_PATH = "extended_graph";

	private static Integer numberOfClusters = null;

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ExtendInformationflowGraphJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Path inputFolder = new Path(args[0]);

		Configuration conf = this.getConf();

		if (args.length > 1) {
			try {
				conf.setDouble("reducer.treshold", Double.valueOf(args[1]));
			} catch (NumberFormatException ex) {
				System.out.println("treshold value is not a Double value " + args[1]);
				return 1;
			}
		}

		if (args.length > 2) {
			try {
				numberOfClusters = Integer.valueOf(args[2]);
			} catch (NumberFormatException ex) {
				System.out.println("Number of clusters couldn't be passed: " + args[2]);
			}
		}
		int res = 0;
		res += runBuildDateVectorJob(conf, inputFolder);

		res += runCalculateDistanceJob(conf, inputFolder);

		res += runExtendInitialGraphJob(conf, inputFolder);

		if (res > 0) {
			System.out.println(res + " Jobs failed");
			return 1;
		}
		return 0;
	}

	private int runBuildDateVectorJob(Configuration conf, Path inputFolder)
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

	private int runCalculateDistanceJob(Configuration conf, Path inputFolder)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job calculateDistanceJob = Job.getInstance(conf);
		calculateDistanceJob.setJobName(this.getClass().getName() + " - CalculateDistanceJob");
		calculateDistanceJob.setJarByClass(ExtendInformationflowGraphJob.class);

		Path dateVectorPath = new Path(inputFolder, TEMP_DATE_VECTOR_DIR);

		calculateDistanceJob.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.setInputDirRecursive(calculateDistanceJob, true);
		FileInputFormat.addInputPath(calculateDistanceJob, dateVectorPath);

		calculateDistanceJob.setMapperClass(Mapper.class);
		calculateDistanceJob.setMapOutputKeyClass(IntWritable.class);
		calculateDistanceJob.setMapOutputValueClass(DateVectorWritable.class);

		calculateDistanceJob.setOutputKeyClass(Text.class);
		calculateDistanceJob.setOutputValueClass(Text.class);

		calculateDistanceJob.setReducerClass(CalculateDistanceReducer.class);

		if (numberOfClusters != null) {
			calculateDistanceJob.setNumReduceTasks(numberOfClusters);
		}
		Path simiilarItemsOutputPath = new Path(inputFolder, SIMILAR_TWITTER_FOLDER);
		HadoopPathUtils.deletePathIfExists(conf, simiilarItemsOutputPath);

		calculateDistanceJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(calculateDistanceJob, simiilarItemsOutputPath);

		return (calculateDistanceJob.waitForCompletion(true) ? 0 : 1);
	}

	private int runExtendInitialGraphJob(Configuration conf, Path inputFolder)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job extendInitialGraphJob = Job.getInstance(conf);
		extendInitialGraphJob.setJobName(this.getClass().getName() + " - ExtendInitialGraphJob");
		extendInitialGraphJob.setJarByClass(ExtendInformationflowGraphJob.class);

		Path similarTweetsFolder = new Path(SIMILAR_TWITTER_FOLDER);

		extendInitialGraphJob.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.setInputDirRecursive(extendInitialGraphJob, true);
		FileInputFormat.addInputPath(extendInitialGraphJob, similarTweetsFolder);

		Path initialGraphPath = new Path(inputFolder, INITIAL_GRAPH_PATH);
		extendInitialGraphJob.setCacheFiles(new URI[] { initialGraphPath.toUri() });

		extendInitialGraphJob.setMapOutputValueClass(Text.class);
		extendInitialGraphJob.setMapOutputKeyClass(Text.class);

		extendInitialGraphJob.setMapperClass(ExtendGraphMapper.class);

		Path extendedGraphPath = new Path(inputFolder, EXTENDED_GRAPH_PATH);

		HadoopPathUtils.deletePathIfExists(conf, extendedGraphPath);
		
		extendInitialGraphJob.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(extendInitialGraphJob, extendedGraphPath);

		extendInitialGraphJob.setOutputKeyClass(NullWritable.class);
		extendInitialGraphJob.setOutputValueClass(Text.class);

		extendInitialGraphJob.setReducerClass(ExtendGraphReducer.class);

		return (extendInitialGraphJob.waitForCompletion(true) ? 0 : 1);
	}

}
