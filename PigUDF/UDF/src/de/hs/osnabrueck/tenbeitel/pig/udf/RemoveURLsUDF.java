package de.hs.osnabrueck.tenbeitel.pig.udf;

import java.io.IOException;

import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class RemoveURLsUDF extends EvalFunc<String> {
	private static final String URL_REGEX = "(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
	
	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		Object text = input.get(0);
		if (text == null) {
			return null;
		}
		return removeURLs((String) text);
	}
	

	private String removeURLs(String text) {
		String textWithoutUrls = text.replaceAll(URL_REGEX, "");
		textWithoutUrls = textWithoutUrls.replaceAll("\\s+", " ");
		return textWithoutUrls;
	}
	
}
