package de.hs.osnabrueck.tenbeitel.mahout;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.util.Version;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

import de.hs.osnabrueck.tenbeitel.mahout.analyzer.GermanAnalyzer;

public class RunKMeans extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RunKMeans(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		int minSupport = 5;
		int minDf = 5;
		int maxDFPercent = 99;
		int maxNGramSize = 1;
		int minLLRValue = 50;
		int reduceTasks = 1;
		int chunkSize = 200;
		int norm = -1;
		boolean sequentialAccessOutput = true;

		String inputDir = args[0];

		Configuration conf = this.getConf();

		String outputDir = args[1];
		Path outputDirPath = new Path(outputDir);
		HadoopUtil.delete(conf, outputDirPath);

		boolean namedVectors = false;
		if (args.length > 2) {
			if (args[2] == "true") {
				namedVectors = true;
			}
		}

		Path tokenizedPath = new Path(outputDir, DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);

		GermanAnalyzer analyzer = new GermanAnalyzer(Version.LUCENE_46);

		CustomDocumentProcessor.tokenizeDocuments(new Path(inputDir), analyzer.getClass().asSubclass(GermanAnalyzer.class),
				tokenizedPath, conf);

		String tfDirName = "tf-vectors";

		DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath, outputDirPath, tfDirName, conf, minSupport,
				maxNGramSize, minLLRValue, norm, true, reduceTasks, chunkSize, sequentialAccessOutput, namedVectors);

		Pair<Long[], List<Path>> docFrequenciesFeatures = TFIDFConverter.calculateDF(new Path(outputDir, tfDirName),
				outputDirPath, conf, chunkSize);

		TFIDFConverter.processTfIdf(new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
				outputDirPath, conf, docFrequenciesFeatures, minDf, maxDFPercent, norm, true, sequentialAccessOutput,
				namedVectors, reduceTasks);

		Path vectorsFolder = new Path(outputDir, "tfidf-vectors");
		Path centroids = new Path(outputDir, "centroids");
		Path clusterOutput = new Path(outputDir, "clusters");

		RandomSeedGenerator.buildRandom(conf, vectorsFolder, centroids, 20, new CosineDistanceMeasure());

		KMeansDriver.run(conf, vectorsFolder, centroids, clusterOutput, 0.01, 20, true, 0, false);

		analyzer.close();

		// Option filePath = SequenceFile.Reader.file(new Path(clusterOutput +
		// "/clusteredPoints" + "/part-m-00000"));
		// try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,
		// filePath)) {
		//
		// IntWritable key = new IntWritable();
		// WeightedPropertyVectorWritable value = new
		// WeightedPropertyVectorWritable();
		//
		// while (reader.next(key, value)) {
		// System.out.println(key.toString() + " belongs to cluster " +
		// value.toString());
		// }
		// }
		return 0;
	}
}
