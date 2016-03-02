package de.hs.osnabrueck.pig.string;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ReplaceNotPrintableUnicodeUDF extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		Object text = input.get(0);
		if (text == null) {
			return null;
		}
		return replaceNotPrintableChars((String) text);
	}

	private String replaceNotPrintableChars(String text) {
		Pattern unicodeOutliers = Pattern.compile("[^\\x00-\\x7F]",
				Pattern.UNICODE_CASE | Pattern.CANON_EQ | Pattern.CASE_INSENSITIVE);
		Matcher unicodeOutlierMatcher = unicodeOutliers.matcher(text);

		return unicodeOutlierMatcher.replaceAll(" ");
	}

}
