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
import de.hs.osnabrueck.tenbeitel.mahout.analyzer.GermanStemAnalyzer;

public class TransformationJob extends Configured implements Tool {
	int minSupport = 5;
	int minDf = 5;
	int maxDFPercent = 99;
	int maxNGramSize = 1;
	int minLLRValue = 50;
	int reduceTasks = 1;
	int chunkSize = 200;
	int norm = -1;
	boolean sequentialAccessOutput = true;
	boolean namedVectors = false;
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TransformationJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		String inputDir = args[0];

		Configuration conf = this.getConf();

		
		System.out.println(args.length);
		if (args.length > 1) {
			namedVectors = Boolean.valueOf(args[1]);
		}

		Path inputDirPath = new Path(inputDir, "sequence_files");
		Path outputDirPath = new Path(inputDir, "transformated_data");
		HadoopUtil.delete(conf, outputDirPath);

		Path tokenizedPath = new Path(outputDirPath, DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);

		GermanStemAnalyzer analyzer = new GermanStemAnalyzer(Version.LUCENE_46);

		DocumentProcessor.tokenizeDocuments(inputDirPath, analyzer.getClass().asSubclass(GermanStemAnalyzer.class),
				tokenizedPath, conf);

		String tfDirName = "tf-vectors";

		DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath, outputDirPath, tfDirName, conf, minSupport,
				maxNGramSize, minLLRValue, norm, true, reduceTasks, chunkSize, sequentialAccessOutput, namedVectors);

		Pair<Long[], List<Path>> docFrequenciesFeatures = TFIDFConverter.calculateDF(new Path(outputDirPath, tfDirName),
				outputDirPath, conf, chunkSize);

		TFIDFConverter.processTfIdf(new Path(outputDirPath, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
				outputDirPath, conf, docFrequenciesFeatures, minDf, maxDFPercent, norm, namedVectors, sequentialAccessOutput,
				namedVectors, reduceTasks);

		analyzer.close();

		return 0;
	}

}
