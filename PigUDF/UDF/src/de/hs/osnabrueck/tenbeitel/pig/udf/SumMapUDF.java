package de.hs.osnabrueck.tenbeitel.pig.udf;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class SumMapUDF extends EvalFunc<Map<String, Integer>> implements Algebraic {
	
	private static TupleFactory tFactory = TupleFactory.getInstance();
	
	@Override
	public Map<String, Integer> exec(Tuple input) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getInitial() {
		return Initial.class.getName();
	}

	@Override
	public String getIntermed() {
		return Intermed.class.getName();
	}

	@Override
	public String getFinal() {
		return Final.class.getName();
	}

	public static class Initial extends EvalFunc<Tuple> {

		@Override
		public Tuple exec(Tuple input) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

	}

	public static class Intermed extends EvalFunc<Tuple> {

		@Override
		public Tuple exec(Tuple input) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

	}

	public static class Final extends EvalFunc<Map<String, Integer>> {

		@Override
		public Map<String, Integer> exec(Tuple input) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

	}

}
