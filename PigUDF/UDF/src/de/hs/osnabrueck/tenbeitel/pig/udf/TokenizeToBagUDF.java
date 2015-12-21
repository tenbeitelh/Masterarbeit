package de.hs.osnabrueck.tenbeitel.pig.udf;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import de.hs.osnabrueck.tenbeitel.tokenizer.CustomGermanAnalyzer;

public class TokenizeToBagUDF extends EvalFunc<DataBag> {

	private static TupleFactory tupleFactory = TupleFactory.getInstance();
	private static BagFactory bagFactory = BagFactory.getInstance();

	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}

		Object text = input.get(0);
		if (text == null) {
			return null;
		}

		TokenStream tStream = createStreamFromString((String) text);
		return createBagFromStream(tStream);
	}

	private DataBag createBagFromStream(TokenStream tStream) throws IOException {
		DataBag resultBag = bagFactory.newDefaultBag();
		CharTermAttribute term = tStream.addAttribute(CharTermAttribute.class);
		tStream.reset();
		while (tStream.incrementToken()) {
			Tuple termTuple = tupleFactory.newTuple(term.toString());
			resultBag.add(termTuple);
		}
		tStream.end();
		tStream.close();

		return resultBag;
	}

	private TokenStream createStreamFromString(String text) throws IOException {
		try (CustomGermanAnalyzer analyzer = new CustomGermanAnalyzer()) {
			TokenStream result = analyzer.tokenStream(null, text);
			result = analyzer.limitTokenLength(result, 3, Integer.MAX_VALUE);
			return result;
		}
	}

}
