package de.hs.osnabrueck.tenbeitel.transactions.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.StringTuple;

import de.hs.osnabrueck.hadoop.util.HadoopPathUtils;
import de.hs.osnabrueck.tenbeitel.transactions.mr.io.WritableWrapper;
import de.hs.osnabrueck.tenbeitel.transactions.mr.mapper.ClusterDataMapper;
import de.hs.osnabrueck.tenbeitel.transactions.mr.mapper.DocumentMapper;
import de.hs.osnabrueck.tenbeitel.transactions.mr.reducer.ClusterDocumentReducer;

public class ClusterToTransactionsJob extends Configured implements Tool {

	private static final String CLUSTER_POINTS_DIR = "kmeans/clusters/clusteredPoints";
	private static final String TOKENIZED_DOC_PATH = "transformated_data/tokenized-documents";
	private static final String BASE_PATH = "apriori/cluster/doc/";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ClusterToTransactionsJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("usage: [inputDir]");
			return 1;
		}
		Path workDir = new Path(args[0]);

		Path clusterPointsPath = new Path(workDir, CLUSTER_POINTS_DIR);
		Path tokenizedDocumentsPath = new Path(workDir, TOKENIZED_DOC_PATH);

		Job createClusterTransactionDir = Job.getInstance(getConf());
		createClusterTransactionDir.setJobName("Create Tokenized Documents by Cluster");
		createClusterTransactionDir.setJarByClass(ClusterToTransactionsJob.class);

		MultipleInputs.addInputPath(createClusterTransactionDir, clusterPointsPath, SequenceFileInputFormat.class,
				ClusterDataMapper.class);
		MultipleInputs.addInputPath(createClusterTransactionDir, tokenizedDocumentsPath, SequenceFileInputFormat.class,
				DocumentMapper.class);

		FileInputFormat.setInputDirRecursive(createClusterTransactionDir, true);

		createClusterTransactionDir.setMapOutputKeyClass(Text.class);
		createClusterTransactionDir.setMapOutputValueClass(WritableWrapper.class);

		createClusterTransactionDir.setReducerClass(ClusterDocumentReducer.class);

		createClusterTransactionDir.setOutputKeyClass(Text.class);
		createClusterTransactionDir.setOutputValueClass(StringTuple.class);

		createClusterTransactionDir.setOutputFormatClass(SequenceFileOutputFormat.class);

		Path outputBasePath = new Path(workDir, BASE_PATH);

		HadoopPathUtils.deletePathIfExists(getConf(), outputBasePath);

		FileOutputFormat.setOutputPath(createClusterTransactionDir, outputBasePath);
		createClusterTransactionDir.setOutputFormatClass(SequenceFileOutputFormat.class);

		return createClusterTransactionDir.waitForCompletion(true) ? 0 : 1;
	}

}
