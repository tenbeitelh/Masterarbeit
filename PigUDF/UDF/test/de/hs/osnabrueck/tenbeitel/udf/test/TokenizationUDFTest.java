package de.hs.osnabrueck.tenbeitel.udf.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hs.osnabrueck.tenbeitel.pig.udf.TokenizationUDF;
import de.hs.osnabrueck.tenbeitel.udf.test.util.TestConstants;

public class TokenizationUDFTest {
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
	public void testTokenizationUDF() throws IOException {
		Tuple input = tupleFactory.newTuple(TestConstants.TOKENIZE_TEST_STRING);
		TokenizationUDF udf = new TokenizationUDF();
		String[] result = udf.exec(input).split(" ");
		String[] expected = new String[] { "Ein", "Satz", "der", "in", "seine", "einzelnen", "Wörter", "unterteilt",
				"wird" };
		assertArrayEquals(expected, result);
	}

}
