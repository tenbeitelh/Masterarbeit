package de.hs.osnabrueck.tenbeitel.mr.association;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AprioriJob extends Configured implements Tool {

	private static final String APRIORI_PATH = "apriori";
	private static final String TEXT_BASED = "textbased";
	private static final String USER_BASED = "userbased";
	private static final String NUMBER_TRANSACTIONS = "number_transactions";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AprioriJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		if (args.length < 5) {
			System.out.println("usage: [inputDir] [outputDir] [minSupport] [minConfidence]");
		}

		Path inputDir = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Double minSupport = Double.valueOf(args[2]);
		Double minConfidence = Double.valueOf(args[3]);

		// MapReduce Job to determine this value
		DetermineTransactionNumberJob transNumberJob = new DetermineTransactionNumberJob();
		int res = transNumberJob.run(conf, inputDir, new Path(outputDir, NUMBER_TRANSACTIONS));
		Double numberOfTransactions = 1000.0;

		conf.setDouble("apriori.min_support", minSupport * numberOfTransactions);
		conf.setDouble("apriori.min_confidence", minConfidence);

		// Todo if() Unterscheidung ob text oder userbasiert, wenn die Formate
		// nicht übereinstimmen

		return 0;
	}

}
