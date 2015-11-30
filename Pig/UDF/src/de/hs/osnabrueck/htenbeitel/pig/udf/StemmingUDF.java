package de.hs.osnabrueck.htenbeitel.pig.udf;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.de.GermanStemFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class StemmingUDF extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		Object text = input.get(0);
		if (text == null) {
			return null;
		}

		return (String) text;
	}

	public String stem(String text) throws IOException {
		StringBuilder stemWords;
		try (Analyzer analyzer = new GermanAnalyzer()) {
			TokenStream tStream = analyzer.tokenStream(null, text);
			CharTermAttribute term = tStream.addAttribute(CharTermAttribute.class);
			GermanStemFilter stemFilter = new GermanStemFilter(tStream);
			stemWords = new StringBuilder();
			while (stemFilter.incrementToken()) {
				stemWords.append(term.toString());
				stemWords.append(" ");
			}
			stemFilter.close();
		}
		return stemWords.toString();
	}

}
