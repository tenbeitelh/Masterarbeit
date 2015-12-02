package de.hs.osnabrueck.htenbeitel.pig.udf;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class StopWordUDF extends EvalFunc<String> {

	@Override
	public String exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0) {
			System.out.println("Tuple is empty");
			return null;
		}
		Object text = tuple.get(0);
		// Set<String> stopWords = initializeStopWords();
		if (text != null) {
			return tokenizeAndFilterStopWords((String) text);
		} else {
			return null;
		}
	}

	// @Override
	// public List<String> getCacheFiles() {
	// List<String> cacheFiles = new ArrayList<String>();
	// cacheFiles.add("/preprocessing/ressource/de_stopwords.txt#stopWords");
	// return cacheFiles;
	// }

	// private Set<String> initializeStopWords() throws IOException {
	// String stopWordsString;
	// try (BufferedReader br = new BufferedReader(new
	// FileReader("stopWords"));) {
	// try {
	// StringBuilder sb = new StringBuilder();
	// String line;
	// while ((line = br.readLine()) != null) {
	// sb.append(line.toLowerCase());
	// }
	// stopWordsString = sb.toString();
	//
	// } catch (Exception e) {
	// throw new IOException("Can't read the file from hdfs", e);
	// }
	// } catch (Exception e) {
	// throw new IOException("Can't open file from hdfs", e);
	// }
	//
	// Set<String> stopWords = new
	// HashSet<String>(Arrays.asList(stopWordsString.split("\r\n")));
	//
	// return stopWords;
	// }

	private String tokenizeAndFilterStopWords(String text) throws IOException {
		Analyzer analyzer = new GermanAnalyzer();
		TokenStream tStream = analyzer.tokenStream(null, text);
		CharTermAttribute term = tStream.addAttribute(CharTermAttribute.class);
		StringBuilder builder = new StringBuilder();
		tStream.reset();
		while (tStream.incrementToken()) {
			builder.append(term.toString());
			builder.append(" ");
		}
		analyzer.close();
		return builder.toString();
	}
}
