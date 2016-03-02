package de.hs.osnabrueck.tenbeitel.udf.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hs.osnabrueck.tenbeitel.pig.udf.TokenizeToBagUDF;
import de.hs.osnabrueck.tenbeitel.udf.test.util.TestConstants;

public class TokenizeToBagUDFTest {
	private static TupleFactory tupleFactory;

	@Before
	public void setUp() throws Exception {
		tupleFactory = TupleFactory.getInstance();
	}

	@After
	public void tearDown() throws Exception {
		tupleFactory = null;
	}

	@Test
	public void test() throws IOException {
		Tuple input = tupleFactory.newTuple(TestConstants.TOKENIZE_TEST_STRING);
		TokenizeToBagUDF udf = new TokenizeToBagUDF();
		assertEquals("{(Ein),(Satz),(der),(seine),(einzelnen),(Wörter),(unterteilt),(wird)}",
				udf.exec(input).toString());
	}

}
