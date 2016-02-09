package de.hs.osnabrueck.tenbeitel.transactions.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

public class ClusterToTransactionsJob extends Configured implements Tool {

	private static final String CLUSTER_POINTS_DIR = "";
	private static final String TOKENIZED_DOC_PATH = null;

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("usage: [inputDir]");
			return 1;
		}
		Path workDir = new Path(args[0]);

		Path clusterPointsPath = new Path(workDir, CLUSTER_POINTS_DIR);
		Path tokenizedDocumentsPath = new Path(workDir, TOKENIZED_DOC_PATH);
		return 0;
	}

}
