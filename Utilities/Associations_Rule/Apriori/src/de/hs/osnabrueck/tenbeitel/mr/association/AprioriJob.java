package de.hs.osnabrueck.tenbeitel.mr.association;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.hs.osnabrueck.tenbeitel.mr.association.io.ItemSetWritable;
import de.hs.osnabrueck.tenbeitel.mr.association.mapper.CreateInitialFrequentItemSetsMapper;
import de.hs.osnabrueck.tenbeitel.mr.association.mapper.FrequentItemsSetMapper;
import de.hs.osnabrueck.tenbeitel.mr.association.reducer.AprioriReducer;

public class AprioriJob extends Configured implements Tool {

	private static final String APRIORI_PATH = "apriori";
	private static final String TEXT_BASED = "textbased";
	private static final String USER_BASED = "userbased";
	private static final String NUMBER_TRANSACTIONS = "number_transactions";
	private static final String ITEMSET_FOLDER = "itemsets";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AprioriJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		if (args.length < 5) {
			System.out.println("usage: [inputDir] [outputDir] [minSupport] [minConfidence] [#transactions]");
		}

		Path inputDir = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Double minSupport = Double.valueOf(args[2]);
		Double minConfidence = Double.valueOf(args[3]);

		Double numberOfTransactions = Double.valueOf(args[4]);

		// MapReduce Job to determine this value, using RecordCounterInstead
		// DetermineTransactionNumberJob transNumberJob = new
		// DetermineTransactionNumberJob();
		// int res = transNumberJob.run(conf, inputDir, new Path(outputDir,
		// NUMBER_TRANSACTIONS));

		conf.setDouble("apriori.min_support", minSupport * numberOfTransactions);
		conf.setDouble("apriori.min_confidence", minConfidence);

		int res = generateItemSets(conf, inputDir, outputDir);

		// while (fs.listStatus(new Path(outputDir, ITEMSET_FOLDER +
		// lengthOfItemSet.toString())).length > 0) {
		//
		// }

		return 0;
	}

	private int generateItemSets(Configuration conf, Path inputDir, Path outputDir)
			throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println(inputDir.toString());
		System.out.println(outputDir.toString());

		FileSystem fs = FileSystem.get(conf);

		Path itemSetPath = new Path(outputDir, ITEMSET_FOLDER);
		System.out.println(itemSetPath.toString());

		Integer lengthOfItemSet = 1;
		// Creating itemsets of length 1
		Job aprioriFirstStep = Job.getInstance(conf);
		aprioriFirstStep.setJobName("Determine frequent itemsets of length = " + lengthOfItemSet.toString());
		aprioriFirstStep.setJarByClass(AprioriJob.class);

		aprioriFirstStep.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(aprioriFirstStep, inputDir);

		aprioriFirstStep.setMapOutputKeyClass(ItemSetWritable.class);
		aprioriFirstStep.setMapOutputValueClass(IntWritable.class);

		aprioriFirstStep.setMapperClass(CreateInitialFrequentItemSetsMapper.class);

		aprioriFirstStep.setOutputKeyClass(NullWritable.class);
		aprioriFirstStep.setOutputValueClass(ItemSetWritable.class);

		aprioriFirstStep.setReducerClass(AprioriReducer.class);

		aprioriFirstStep.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(aprioriFirstStep, new Path(itemSetPath, lengthOfItemSet.toString()));

		int res = aprioriFirstStep.waitForCompletion(true) ? 0 : 1;

		lengthOfItemSet++;

		conf.setInt("apriori.itemset_lenght", lengthOfItemSet);

		Job iterationJob = Job.getInstance(conf);
		iterationJob.setJobName("Determine frequent itemsets of length = " + lengthOfItemSet.toString());
		iterationJob.setJarByClass(AprioriJob.class);

		iterationJob.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(iterationJob, inputDir);

		iterationJob.setCacheFiles(getResultFilesFrom(fs, itemSetPath, lengthOfItemSet - 1));

		iterationJob.setMapOutputKeyClass(ItemSetWritable.class);
		iterationJob.setMapOutputValueClass(IntWritable.class);

		iterationJob.setMapperClass(FrequentItemsSetMapper.class);

		iterationJob.setOutputKeyClass(NullWritable.class);
		iterationJob.setOutputValueClass(ItemSetWritable.class);

		iterationJob.setReducerClass(AprioriReducer.class);

		iterationJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(iterationJob, new Path(itemSetPath, lengthOfItemSet.toString()));

		res += iterationJob.waitForCompletion(true) ? 0 : 1;

		if (res > 0) {
			return 1;
		}
		return res;
	}

	private URI[] getResultFilesFrom(FileSystem fs, Path itemSetPath, Integer previousLengthOfItemSet)
			throws IOException {
		// TODO Auto-generated method stub

		Path globPath = new Path(new Path(itemSetPath, previousLengthOfItemSet.toString()), "part-r-[0-9]*");

		FileStatus[] files = fs.globStatus(globPath);
		URI[] filePathAsURI = new URI[files.length];

		for (int i = 0; i < files.length; i++) {
			filePathAsURI[i] = files[i].getPath().toUri();
		}

		return filePathAsURI;
	}

}
