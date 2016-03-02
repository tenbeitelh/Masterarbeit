package de.hs.osnabrueck.tenbeitel.pig.udf;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import de.hs.osnabrueck.tenbeitel.pig.udf.utils.PigConstants;
import de.hs.osnabrueck.tenbeitel.tokenizer.CustomGermanAnalyzer;

public class StopWordUDF extends EvalFunc<String> {

	@Override
	public String exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0) {
			System.out.println("Tuple is empty");
			return null;
		}
		Object text = tuple.get(0);
		if (text != null) {
			return tokenizeAndFilterStopWords((String) text);
		} else {
			return null;
		}
	}

	private String tokenizeAndFilterStopWords(String text) throws IOException {
		StringBuilder builder;
		try (CustomGermanAnalyzer analyzer = new CustomGermanAnalyzer()) {
			TokenStream tStream = analyzer.tokenStream(null, text);
			CharTermAttribute term = tStream.addAttribute(CharTermAttribute.class);
			tStream = analyzer.tokenStreamToLowerCase(tStream);
			tStream = analyzer.removeStopWordsFromTokenStream(tStream);

			builder = new StringBuilder();
			tStream.reset();
			while (tStream.incrementToken()) {
				builder.append(term.toString());
				builder.append(PigConstants.TOKEN_SEPERATOR);
			}
			tStream.end();
			tStream.close();

		}
		return builder.toString();

	}
}
