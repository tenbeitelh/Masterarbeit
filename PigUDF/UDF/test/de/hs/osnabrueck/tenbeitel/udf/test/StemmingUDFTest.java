package de.hs.osnabrueck.tenbeitel.udf.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hs.osnabrueck.tenbeitel.pig.udf.StemmingUDF;
import de.hs.osnabrueck.tenbeitel.udf.test.util.TestConstants;

public class StemmingUDFTest {

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
	public void testStemming() throws IOException {
		Tuple input = tupleFactory.newTuple(TestConstants.STEMMING_TEST_STRING);
		StemmingUDF udf = new StemmingUDF();
		assertEquals("test test vergleich vergleich", udf.exec(input).trim());
	}

}
