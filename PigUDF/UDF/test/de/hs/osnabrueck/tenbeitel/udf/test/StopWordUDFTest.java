package de.hs.osnabrueck.tenbeitel.udf.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hs.osnabrueck.tenbeitel.pig.udf.StopWordUDF;
import de.hs.osnabrueck.tenbeitel.udf.test.util.TestConstants;

public class StopWordUDFTest {
	
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
	public void testStopWordUDF() throws IOException {
		StopWordUDF stopUdf = new StopWordUDF();
		Tuple input = tupleFactory.newTuple(TestConstants.STOP_WORD_STRING);
		assertEquals("entfernung stopwörtern", stopUdf.exec(input).trim());
	}

}
