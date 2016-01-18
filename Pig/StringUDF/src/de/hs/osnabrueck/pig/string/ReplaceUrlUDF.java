package de.hs.osnabrueck.pig.string;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ReplaceUrlUDF extends EvalFunc<String> {

	private static final String URL_REGEX = "(https?|ftp|file):/{0,2}[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";

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
		return text.replaceAll(URL_REGEX, " ");
	}

}
