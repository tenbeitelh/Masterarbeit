package de.hs.osanbrueck.tenbeitel.transformation;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

import de.hs.osnabrueck.tenbeitel.mahout.analyzer.GermanAnalyzer;

public class TransformationJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TransformationJob(), args);
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
		System.out.println(args.length);
		if (args.length > 2) {
			System.out.println(args[2]);
			namedVectors = Boolean.valueOf(args[2]);
		}

		Path tokenizedPath = new Path(outputDir, DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);

		GermanAnalyzer analyzer = new GermanAnalyzer(Version.LUCENE_46);

		DocumentProcessor.tokenizeDocuments(new Path(inputDir), analyzer.getClass().asSubclass(GermanAnalyzer.class),
				tokenizedPath, conf);

		String tfDirName = "tf-vectors";

		DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath, outputDirPath, tfDirName, conf, minSupport,
				maxNGramSize, minLLRValue, norm, true, reduceTasks, chunkSize, sequentialAccessOutput, namedVectors);

		Pair<Long[], List<Path>> docFrequenciesFeatures = TFIDFConverter.calculateDF(new Path(outputDir, tfDirName),
				outputDirPath, conf, chunkSize);

		TFIDFConverter.processTfIdf(new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
				outputDirPath, conf, docFrequenciesFeatures, minDf, maxDFPercent, norm, true, sequentialAccessOutput,
				namedVectors, reduceTasks);

		analyzer.close();

		return 0;
	}

}
