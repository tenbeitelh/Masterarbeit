package de.hs.osnabrueck.tenbeitel.mr.association;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.StringTuple;

import de.hs.osnabrueck.hadoop.util.HadoopPathUtils;
import de.hs.osnabrueck.tenbeitel.mr.association.mapper.CreateInitialFrequentItemSetsMapper;
import de.hs.osnabrueck.tenbeitel.mr.association.mapper.KFrequentItemsSetMapper;
import de.hs.osnabrueck.tenbeitel.mr.association.mapper.KRuleMapper;
import de.hs.osnabrueck.tenbeitel.mr.association.reducer.AprioriReducer;
import de.hs.osnabrueck.tenbeitel.mr.association.reducer.KRuleReducer;
import de.hs.osnabrueck.tenbeitel.mr.association.utils.AprioriFileUtils;

public class AprioriJob extends Configured implements Tool {

	private static final String ITEMSET_FOLDER = "itemsets";
	private static final String RULE_PATH = "rules";
	private static Integer lastItemset = 0;

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AprioriJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		if (args.length < 4) {
			System.out.println("usage: [inputDir] [outputDir] [minSupport] [minConfidence] [#transactions]");
			System.out.println("usage: [inputDir] [outputDir] [minSupport*#transactions] [minConfidence]");
		}

		Path inputDir = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		HadoopPathUtils.deletePathIfExists(conf, outputDir);

		Double minSupport = Double.valueOf(args[2]);

		Double minConfidence = Double.valueOf(args[3]);
		if (args.length == 5) {
			Double numberOfTransactions = Double.valueOf(args[4]);
			conf.setDouble("apriori.min_support", minSupport * numberOfTransactions);
		} else {
			conf.setDouble("apriori.min_support", minSupport);
		}

		conf.setDouble("apriori.min_confidence", minConfidence);

		FileSystem fs = FileSystem.get(conf);

		int res = generateItemSets(fs, conf, inputDir, outputDir);
		if (lastItemset > 1) {
			res = generateRules(fs, conf, new Path(outputDir, ITEMSET_FOLDER), new Path(outputDir, RULE_PATH));
		}
		if (res > 0) {
			return 1;
		}
		return res;
	}

	private int generateRules(FileSystem fs, Configuration conf, Path inputDir, Path outputDir)
			throws IOException, ClassNotFoundException, InterruptedException {

		Job aprioriRuleGen = Job.getInstance(conf);
		aprioriRuleGen.setJobName("Generate rules from itemsets");
		aprioriRuleGen.setJarByClass(AprioriJob.class);

		aprioriRuleGen.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(aprioriRuleGen, inputDir);
		FileInputFormat.setInputDirRecursive(aprioriRuleGen, true);

		aprioriRuleGen.setMapOutputKeyClass(StringTuple.class);
		aprioriRuleGen.setMapOutputValueClass(IntWritable.class);

		aprioriRuleGen.setMapperClass(KRuleMapper.class);

		aprioriRuleGen.setOutputKeyClass(Text.class);
		aprioriRuleGen.setOutputValueClass(DoubleWritable.class);

		aprioriRuleGen.setReducerClass(KRuleReducer.class);

		aprioriRuleGen.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(aprioriRuleGen, outputDir);

		aprioriRuleGen.setCacheFiles(getAllItemSetFiles(fs, inputDir, lastItemset));

		return aprioriRuleGen.waitForCompletion(true) ? 0 : 1;
	}

	private int generateItemSets(FileSystem fs, Configuration conf, Path inputDir, Path outputDir)
			throws IOException, ClassNotFoundException, InterruptedException {

		Path itemSetPath = new Path(outputDir, ITEMSET_FOLDER);

		Integer lengthOfItemSet = 1;
		// Creating itemsets of length 1
		Job aprioriFirstStep = Job.getInstance(conf);
		aprioriFirstStep.setJobName("Determine frequent itemsets of length = " + lengthOfItemSet.toString());
		aprioriFirstStep.setJarByClass(AprioriJob.class);

		aprioriFirstStep.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(aprioriFirstStep, inputDir);

		aprioriFirstStep.setMapOutputKeyClass(StringTuple.class);
		aprioriFirstStep.setMapOutputValueClass(IntWritable.class);

		aprioriFirstStep.setMapperClass(CreateInitialFrequentItemSetsMapper.class);

		aprioriFirstStep.setOutputKeyClass(StringTuple.class);
		aprioriFirstStep.setOutputValueClass(IntWritable.class);

		aprioriFirstStep.setReducerClass(AprioriReducer.class);

		aprioriFirstStep.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(aprioriFirstStep, new Path(itemSetPath, lengthOfItemSet.toString()));
		System.out.println("Calculating itemsets of length: " + lengthOfItemSet);
		int res = aprioriFirstStep.waitForCompletion(true) ? 0 : 1;

		while (AprioriFileUtils.previousItemSetsExists(conf, new Path(itemSetPath, lengthOfItemSet.toString()))) {
			lengthOfItemSet++;

			conf.setInt("apriori.itemset_lenght", lengthOfItemSet);

			Job iterationJob = Job.getInstance(conf);
			iterationJob.setJobName("Determine frequent itemsets of length = " + lengthOfItemSet.toString());
			iterationJob.setJarByClass(AprioriJob.class);

			iterationJob.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(iterationJob, inputDir);

			iterationJob.setCacheFiles(getResultFilesFrom(fs, itemSetPath, lengthOfItemSet - 1));

			iterationJob.setMapOutputKeyClass(StringTuple.class);
			iterationJob.setMapOutputValueClass(IntWritable.class);

			iterationJob.setMapperClass(KFrequentItemsSetMapper.class);

			iterationJob.setOutputKeyClass(StringTuple.class);
			iterationJob.setOutputValueClass(IntWritable.class);

			iterationJob.setReducerClass(AprioriReducer.class);

			iterationJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileOutputFormat.setOutputPath(iterationJob, new Path(itemSetPath, lengthOfItemSet.toString()));

			System.out.println("Calculating itemsets of length: " + lengthOfItemSet);

			res += iterationJob.waitForCompletion(true) ? 0 : 1;
		}
		// -2 because the last generated itemset is empty
		lastItemset = lengthOfItemSet - 1;
		if (res > 0) {
			return 1;
		}
		return res;
	}

	private URI[] getResultFilesFrom(FileSystem fs, Path itemSetPath, Integer previousLengthOfItemSet)
			throws IOException {
		Path globPath = new Path(new Path(itemSetPath, previousLengthOfItemSet.toString()), "part-r-[0-9]*");

		FileStatus[] files = fs.globStatus(globPath);
		URI[] filePathAsURI = new URI[files.length];

		for (int i = 0; i < files.length; i++) {
			filePathAsURI[i] = files[i].getPath().toUri();
		}

		return filePathAsURI;
	}

	private URI[] getAllItemSetFiles(FileSystem fs, Path itemSetPath, Integer lastItemSetNumber) throws IOException {
		System.out.println(lastItemSetNumber);
		List<URI> uriList = new ArrayList<URI>();
		for (Integer i = 1; i <= lastItemSetNumber; i++) {
			System.out.println(new Path(new Path(itemSetPath, i.toString()).toString()));
			Path globPath = new Path(new Path(itemSetPath, i.toString()), "part-r-[0-9]*");
			System.out.println(globPath.toString());
			FileStatus[] files = fs.globStatus(globPath);
			System.out.println(Arrays.toString(files));
			for (FileStatus file : files) {
				uriList.add(file.getPath().toUri());
			}
		}
		URI[] tempArray = new URI[uriList.size()];
		tempArray = uriList.toArray(tempArray);
		System.out.println(Arrays.toString(tempArray));
		return tempArray;

	}
}
