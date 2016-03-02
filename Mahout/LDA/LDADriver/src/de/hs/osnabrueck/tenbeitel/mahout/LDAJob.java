package de.hs.osnabrueck.tenbeitel.mahout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.lda.cvb.CVB0Driver;

public class LDAJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new LDAJob(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = this.getConf();

		String inputPath = args[0];
		Path inputDir = new Path(inputPath);

		CVB0Driver ldaDriver = new CVB0Driver();
		// ldaDriver.run(conf, inputPath, topicModelOutputPath, numTopics,
		// numTerms, alpha, eta, maxIterations, iterationBlockSize,
		// convergenceDelta, dictionaryPath, docTopicOutputPath,
		// topicModelStateTempPath, randomSeed, testFraction, numTrainThreads,
		// numUpdateThreads, maxItersPerDoc, numReduceTasks, backfillPerplexity)
		
		return 0;
	}

}
