package de.hs.osnabrueck.tenbeitel.pig.udf;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.bag.HashBag;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import de.hs.osnabrueck.tenbeitel.pig.udf.utils.PigConstants;

public class TermFrequencyUDF extends EvalFunc<Map<String, Integer>> {

	@Override
	public Map<String, Integer> exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		Object text = input.get(0);
		if (text == null) {
			return null;
		}
		
		return calculateTermFrequency((String) text);
	}

	private Map<String, Integer> calculateTermFrequency(String text) {
		Map<String, Integer> resultMap = new HashMap<String, Integer>();
		String[] termArray = text.split(PigConstants.TOKEN_SEPERATOR);
		List<String> termList = Arrays.asList(termArray);
		Bag bag = new HashBag(termList);

		Set<String> distinctTerms = new HashSet<String>(termList);
		for (String term : distinctTerms) {
			int objectCount = bag.getCount(term);

			resultMap.put(term, objectCount);

		}

		return resultMap;

	}

}
