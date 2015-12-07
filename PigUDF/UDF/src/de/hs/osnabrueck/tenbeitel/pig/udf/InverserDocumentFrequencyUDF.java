package de.hs.osnabrueck.tenbeitel.pig.udf;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import de.hs.osnabrueck.tenbeitel.pig.udf.utils.PigConstants;

public class InverserDocumentFrequencyUDF extends EvalFunc<Map<String, Double>> {

	@Override
	public Map<String, Double> exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		Object records = input.get(0);
		if (records == null) {
			return null;
		}
		DataBag bag = (DataBag) records;
		return calculateInverseDocumentFrequency(bag);
	}

	public Map<String, Double> calculateInverseDocumentFrequency(DataBag bag) throws IOException {
		Long documentCount = bag.size();
		Set<String> distinctTokens = getDistinctTokensFromBag(bag);
		Map<String, Integer> documentFrequencies = calculateDocumentFrequencies(distinctTokens, bag);
		Map<String, Double> inverseDocumentFrequencies = new HashMap<String, Double>();
		for (String term : documentFrequencies.keySet()) {
			Double tdf = Math.log10((documentCount) / (documentFrequencies.get(term) + 1));
			inverseDocumentFrequencies.put(term, tdf);
		}
		return inverseDocumentFrequencies;
	}

	private Map<String, Integer> calculateDocumentFrequencies(Set<String> distinctTokens, DataBag bag)
			throws IOException {
		Map<String, Integer> dfMap = new HashMap<String, Integer>();
		for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
			String text = (String) it.next().get(0);
			for (String term : distinctTokens) {
				if (text.contains(term)) {
					if (dfMap.containsKey(term)) {
						dfMap.put(term, dfMap.get(term) + 1);
					} else {
						dfMap.put(term, 1);
					}
				}
			}
		}
		return dfMap;
	}

	private Set<String> getDistinctTokensFromBag(DataBag bag) throws IOException {
		Set<String> distinctTokens = new HashSet<String>();
		for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
			String text = (String) it.next().get(0);
			distinctTokens.addAll(Arrays.asList(text.split(PigConstants.TOKEN_SEPERATOR)));
		}

		return distinctTokens;
	}

}
