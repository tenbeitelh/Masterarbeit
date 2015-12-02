package de.hs.osnabrueck.htenbeitel.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class StringLength extends EvalFunc<Integer> {

	@Override
	public Integer exec(Tuple input) throws IOException {
		// TODO Auto-generated method stub
		if (input == null || input.size() == 0) {
			return null;
		}
		Object text = input.get(0);
		if (text == null) {
			return null;
		}
		return ((String) text).length();
	}

}
