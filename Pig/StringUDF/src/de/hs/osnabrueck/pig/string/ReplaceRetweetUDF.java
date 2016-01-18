package de.hs.osnabrueck.pig.string;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ReplaceRetweetUDF extends EvalFunc<String> {
	
	private static final String RETWEET_REGEX = "RT";

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		Object text = input.get(0);
		if (text == null) {
			return null;
		}
		return removeRetweets((String) text);
	}

	private String removeRetweets(String text) {
		// TODO Auto-generated method stub
		return text.replaceAll(RETWEET_REGEX, "");
	}

}
