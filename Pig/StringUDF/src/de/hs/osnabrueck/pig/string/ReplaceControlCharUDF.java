package de.hs.osnabrueck.pig.string;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.google.common.base.CharMatcher;

public class ReplaceControlCharUDF extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		Object text = input.get(0);
		if (text == null) {
			return null;
		}
		return replaceControlCharacters((String) text);
	}

	private String replaceControlCharacters(String text) {
		String replaced = text.replaceAll("[\u0000-\u001f]", " ");
		replaced = CharMatcher.JAVA_ISO_CONTROL.replaceFrom(replaced, " ");
		return replaced.replaceAll("^ +| +$|( )+", " ");
	}

}
