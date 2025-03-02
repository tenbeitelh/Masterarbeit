package de.hs.osnabrueck.tenbeitel.mahout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.CosineDistanceMeasure;

public class KMeansJob extends Configured implements Tool {
	private static int k = 20;
	private static int maxIterations = 20;
	private static double convergenceDelta = 0.01;

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new KMeansJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("Usage: [inputDir] [k]");
		}
		String inputPath = args[0];

		// Get value of k by commandline argument;
		if (args.length > 1) {
			try {
				k = Integer.parseInt(args[1]);
			} catch (NumberFormatException ex) {
				System.out.println("Could not parse integer from commandline: " + args[1]);
				System.out.println("Setting k to default: " + k);
			}
		}
		

		Configuration conf = this.getConf();

		Path inputDir = new Path(inputPath, "transformated_data");
		Path outputDir = new Path(inputPath, "kmeans" + k);
		HadoopUtil.delete(conf, outputDir);
		Path vectorsFolder = new Path(inputDir, "tfidf-vectors");
		Path centroids = new Path(outputDir, "centroids");
		Path clusterOutput = new Path(outputDir, "clusters");

		RandomSeedGenerator.buildRandom(conf, vectorsFolder, centroids, k, new CosineDistanceMeasure());

		KMeansDriver.run(conf, vectorsFolder, centroids, clusterOutput, convergenceDelta, maxIterations, true, 0,
				false);

		return 0;
	}

}
