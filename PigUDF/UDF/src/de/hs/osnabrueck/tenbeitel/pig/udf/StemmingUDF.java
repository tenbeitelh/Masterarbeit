package de.hs.osnabrueck.tenbeitel.pig.udf;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import de.hs.osnabrueck.tenbeitel.pig.udf.utils.PigConstants;
import de.hs.osnabrueck.tenbeitel.tokenizer.CustomGermanAnalyzer;
import de.hs.osnabrueck.tenbeitel.tokenizer.enumeration.StemmingType;

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
		try (CustomGermanAnalyzer analyzer = new CustomGermanAnalyzer()) {
			TokenStream tStream = analyzer.tokenStream(null, text);
			CharTermAttribute term = tStream.addAttribute(CharTermAttribute.class);
			tStream = analyzer.tokenStreamToLowerCase(tStream);
			tStream = analyzer.stemTokenStream(tStream, StemmingType.GermanStemFilter);
			tStream.reset();
			stemWords = new StringBuilder();
			while (tStream.incrementToken()) {
				stemWords.append(term.toString());
				stemWords.append(PigConstants.TOKEN_SEPERATOR);
			}
			tStream.end();
			tStream.close();
		}
		return stemWords.toString();
	}

}
