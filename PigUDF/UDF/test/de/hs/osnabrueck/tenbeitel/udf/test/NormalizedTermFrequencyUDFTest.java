package de.hs.osnabrueck.tenbeitel.udf.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hs.osnabrueck.tenbeitel.pig.udf.NormalizedTermFrequencyUDF;
import de.hs.osnabrueck.tenbeitel.udf.test.util.TestConstants;

public class NormalizedTermFrequencyUDFTest {
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
	public void testExecTuple() throws IOException {
		Tuple input = tupleFactory.newTuple(TestConstants.TF_TEST_STRING);
		NormalizedTermFrequencyUDF udf = new NormalizedTermFrequencyUDF();
		assertEquals("{(Hallo,0.5),(Satz,0.2),(Test,0.3)}", udf.exec(input).toString());
	}

}
