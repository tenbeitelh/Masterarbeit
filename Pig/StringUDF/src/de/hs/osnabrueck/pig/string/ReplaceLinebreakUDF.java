package de.hs.osnabrueck.pig.string;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ReplaceLinebreakUDF extends EvalFunc<String> {

	private static final String LINEBREAK_REGEX = "\\r+|\\n+";

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		Object text = input.get(0);
		if (text == null) {
			return null;
		}
		return removeLinebreacks((String) text);
	}

	private String removeLinebreacks(String text) {
		return text.replaceAll(LINEBREAK_REGEX, " ");
	}

}
