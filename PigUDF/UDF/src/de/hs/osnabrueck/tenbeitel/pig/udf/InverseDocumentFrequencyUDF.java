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

/**
 * Calculates the Invererse Document Frequency(IDF) on an Map of terms
 * 
 * @author H. Tenbeitel
 *
 */
public class InverseDocumentFrequencyUDF extends EvalFunc<Map<String, Double>> {
   
	@Override
	public Map<String, Double> exec(Tuple input) throws IOException {

		if (input == null || input.size() < 2) {
			return null;
		}

		Object map = input.get(0);
		Object docCount = input.get(1);
		if (map == null || docCount == null) {
			return null;
		}

		if (!(map instanceof Map<?, ?>)) {
			return null;
		}

		return calculateInverseDocumentFrequency((Map<String, Integer>) map, (Integer) docCount);

	}

	private Map<String, Double> calculateInverseDocumentFrequency(Map<String, Integer> map, Integer docCount) {
		Map<String, Double> idfMap = new HashMap<String, Double>();
		for(String term : map.keySet()){
			Integer occurenceCount = map.get(term);
			Double idfValue = Math.log10((docCount*1.0)/(occurenceCount+1));
			idfMap.put(term, idfValue);
		}
		return idfMap;
	}

	/*
	 * First draft
	 * 
	 * public Map<String, Double> calculateInverseDocumentFrequency(DataBag bag)
	 * throws IOException { Long documentCount = bag.size(); Set<String>
	 * distinctTokens = getDistinctTokensFromBag(bag); Map<String, Integer>
	 * documentFrequencies = calculateDocumentFrequencies(distinctTokens, bag);
	 * Map<String, Double> inverseDocumentFrequencies = new HashMap<String,
	 * Double>(); for (String term : documentFrequencies.keySet()) { Double tdf
	 * = Math.log10((documentCount) / (documentFrequencies.get(term) + 1));
	 * inverseDocumentFrequencies.put(term, tdf); } return
	 * inverseDocumentFrequencies; }
	 * 
	 * private Map<String, Integer> calculateDocumentFrequencies(Set<String>
	 * distinctTokens, DataBag bag) throws IOException { Map<String, Integer>
	 * dfMap = new HashMap<String, Integer>(); for (Iterator<Tuple> it =
	 * bag.iterator(); it.hasNext();) { String text = (String) it.next().get(0);
	 * for (String term : distinctTokens) { if (text.contains(term)) { if
	 * (dfMap.containsKey(term)) { dfMap.put(term, dfMap.get(term) + 1); } else
	 * { dfMap.put(term, 1); } } } } return dfMap; }
	 * 
	 * private Set<String> getDistinctTokensFromBag(DataBag bag) throws
	 * IOException { Set<String> distinctTokens = new HashSet<String>(); for
	 * (Iterator<Tuple> it = bag.iterator(); it.hasNext();) { String text =
	 * (String) it.next().get(0);
	 * distinctTokens.addAll(Arrays.asList(text.split(PigConstants.
	 * TOKEN_SEPERATOR))); }
	 * 
	 * return distinctTokens; }
	 */

}
