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
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import de.hs.osnabrueck.tenbeitel.pig.udf.utils.PigConstants;

public class NormalizedTermFrequencyUDF extends EvalFunc<DataBag> {
	private static BagFactory bagFactory = BagFactory.getInstance();
	private static TupleFactory tupleFactory = TupleFactory.getInstance();

	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		Object text = input.get(0);
		if (text == null) {
			return null;
		}

		return calculateNormalizedTermFrequency((String) text);
	}

	private DataBag calculateNormalizedTermFrequency(String text) throws ExecException {
		DataBag resultBag = bagFactory.newDefaultBag();
		String[] termArray = text.split(PigConstants.TOKEN_SEPERATOR);

		int bagSize = termArray.length;
		List<String> termList = Arrays.asList(termArray);

		Bag bag = new HashBag(termList);

		Set<String> distinctTerms = new HashSet<String>(termList);
		for (String term : distinctTerms) {
			float objectCount = bag.getCount(term);
			float frequency = objectCount / bagSize;

			Tuple tuple = tupleFactory.newTuple(2);
			tuple.set(0, term);
			tuple.set(1, frequency);
			resultBag.add(tuple);
		}

		return resultBag;

	}

}
