package de.hs.osnabrueck.tenbeitel.mr.twittergraph;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.hs.osnabrueck.hadoop.util.HadoopPathUtils;
import de.hs.osnabrueck.tenbeitel.mr.twittergraph.io.TwitterWritable;
import de.hs.osnabrueck.tenbeitel.mr.twittergraph.mapper.ClusterDataJoinMapper;
import de.hs.osnabrueck.tenbeitel.mr.twittergraph.mapper.TwitterDataJoinMapper;
import de.hs.osnabrueck.tenbeitel.mr.twittergraph.reducer.TwitterClusterJoinReducer;

public class TwitterDataGraphJob extends Configured implements Tool {

	private static final String TWITTER_DETAILED_FOLDER = "tweet_detailed";
	private static final String CLUSTER_FOLDER_PATH = "kmeans/clusters/clusteredPoints";
	private static final String GRAPH_PATH = "twitter_graph";
	private static final String INITIAL_GRAPH_PATH = "graph_data/part-r-00000";;

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TwitterDataGraphJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = this.getConf();

		Path inputFolder = new Path(args[0]);

		int res = 0;

		res += runJoinTwitterAndClusterDataJob(conf, inputFolder);

		return res;
	}

	private int runJoinTwitterAndClusterDataJob(Configuration conf, Path inputFolder)
			throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Job joinTwitterAndClusterData = Job.getInstance(conf);
		joinTwitterAndClusterData.setJarByClass(TwitterDataGraphJob.class);
		joinTwitterAndClusterData.setJobName("Join Twitter and Cluster Data");

		Path twitterDataPath = new Path(inputFolder, TWITTER_DETAILED_FOLDER);
		Path clusterDataPath = new Path(inputFolder, CLUSTER_FOLDER_PATH);

		MultipleInputs.addInputPath(joinTwitterAndClusterData, twitterDataPath, TextInputFormat.class,
				TwitterDataJoinMapper.class);
		MultipleInputs.addInputPath(joinTwitterAndClusterData, clusterDataPath, SequenceFileInputFormat.class,
				ClusterDataJoinMapper.class);

		FileInputFormat.setInputDirRecursive(joinTwitterAndClusterData, true);

		joinTwitterAndClusterData.setMapOutputKeyClass(Text.class);
		joinTwitterAndClusterData.setMapOutputValueClass(TwitterWritable.class);

		joinTwitterAndClusterData.setOutputKeyClass(NullWritable.class);
		joinTwitterAndClusterData.setOutputValueClass(Text.class);

		Path initialGraphPath = new Path(inputFolder, INITIAL_GRAPH_PATH);
		joinTwitterAndClusterData.setCacheFiles(new URI[] { initialGraphPath.toUri() });

		joinTwitterAndClusterData.setReducerClass(TwitterClusterJoinReducer.class);

		joinTwitterAndClusterData.setOutputFormatClass(TextOutputFormat.class);

		Path twitterGraphPath = new Path(inputFolder, GRAPH_PATH);

		HadoopPathUtils.deletePathIfExists(conf, twitterGraphPath);
		FileOutputFormat.setOutputPath(joinTwitterAndClusterData, twitterGraphPath);

		return (joinTwitterAndClusterData.waitForCompletion(true) ? 0 : 1);
	}

}
