package de.hs.osnabrueck.tenbeitel.pig.udf;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import de.hs.osnabrueck.tenbeitel.pig.udf.utils.PigConstants;

public class DocumentTermOccurenceUDF extends EvalFunc<Map<String, Integer>> {

	@Override
	public Map<String, Integer> exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		Object text = input.get(0);
		if (text == null) {
			return null;
		}

		return termsInDocuments((String) text);
	}

	private Map<String, Integer> termsInDocuments(String text) {
		Map<String, Integer> termCountMap = new HashMap<String, Integer>();
		Set<String> terms = new HashSet<String>(Arrays.asList(text.split(PigConstants.TOKEN_SEPERATOR)));
		for (String s : terms) {
			termCountMap.put(s, 1);
		}
		return termCountMap;
	}

}
