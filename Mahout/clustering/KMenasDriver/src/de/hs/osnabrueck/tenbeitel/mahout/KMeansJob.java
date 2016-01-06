package de.hs.osnabrueck.tenbeitel.mahout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.CosineDistanceMeasure;

public class KMeansJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new KMeansJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		String outputDir = args[0];

		Configuration conf = this.getConf();

		Path vectorsFolder = new Path(outputDir, "tfidf-vectors");
		Path centroids = new Path(outputDir, "centroids");
		Path clusterOutput = new Path(outputDir, "clusters");

		RandomSeedGenerator.buildRandom(conf, vectorsFolder, centroids, 20, new CosineDistanceMeasure());

		KMeansDriver.run(conf, vectorsFolder, centroids, clusterOutput, 0.01, 20, true, 0, false);

		return 0;
	}

}
