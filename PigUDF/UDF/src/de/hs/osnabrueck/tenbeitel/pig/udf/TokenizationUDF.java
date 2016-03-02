package de.hs.osnabrueck.tenbeitel.pig.udf;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import de.hs.osnabrueck.tenbeitel.pig.udf.utils.PigConstants;
import de.hs.osnabrueck.tenbeitel.tokenizer.CustomGermanAnalyzer;

public class TokenizationUDF extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		// TODO Auto-generated method stub
		if (input == null || input.size() == 0) {
			return null;
		}
		Object text = input.get(0);
		if (text == null) {
			return null;
		}

		return tokenizeString((String) text);

	}

	private String tokenizeString(String text) throws IOException {
		StringBuilder builder;
		try (CustomGermanAnalyzer analyzer = new CustomGermanAnalyzer()) {
			TokenStream tStream = analyzer.tokenStream(null, text);
			CharTermAttribute term = tStream.addAttribute(CharTermAttribute.class);

			tStream.reset();
			builder = new StringBuilder();
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
