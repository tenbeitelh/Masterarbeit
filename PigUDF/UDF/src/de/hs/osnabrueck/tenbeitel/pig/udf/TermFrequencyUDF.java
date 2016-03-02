<<<<<<< HEAD
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
=======
package de.hs.osnabrueck.tenbeitel.pig.udf;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.bag.HashBag;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import de.hs.osnabrueck.tenbeitel.pig.udf.utils.PigConstants;

public class TermFrequencyUDF extends EvalFunc<DataBag> {
	private static final BagFactory bagFactory = BagFactory.getInstance();
	private static final TupleFactory tupleFactory = TupleFactory.getInstance();

	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		Object text = input.get(0);
		if (text == null) {
			return null;
		}

		return calculateTermFrequency((String) text);
	}

	private DataBag calculateTermFrequency(String text) throws ExecException {
		DataBag resultBag = bagFactory.newDefaultBag();

		String[] termArray = text.split(PigConstants.TOKEN_SEPERATOR);
		List<String> termList = Arrays.asList(termArray);
		Bag bag = new HashBag(termList);

		Set<String> distinctTerms = new HashSet<String>(termList);
		for (String term : distinctTerms) {
			int objectCount = bag.getCount(term);

			Tuple tuple = tupleFactory.newTuple(2);
			tuple.set(0, term);
			tuple.set(1, objectCount);
			resultBag.add(tuple);
		}

		return resultBag;

	}

}
>>>>>>> fdfee6bca5ef6de15c2654e6fc072da1f0681430
