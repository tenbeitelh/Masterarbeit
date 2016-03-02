package de.hs.osnabrueck.tenbeitel.udf.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.hs.osnabrueck.tenbeitel.pig.udf.RemoveURLsUDF;
import de.hs.osnabrueck.tenbeitel.udf.test.util.TestConstants;

public class RemoveUrlUDFTest {

	private static TupleFactory tupleFactory;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {

	}

	@Before
	public void setUp() throws Exception {
		tupleFactory = TupleFactory.getInstance();
	}

	@After
	public void tearDown() throws Exception {
		tupleFactory = null;
	}

	@Test
	public void testRemoveURL() throws IOException {
		RemoveURLsUDF urlUDF = new RemoveURLsUDF();
		Tuple input = tupleFactory.newTuple(TestConstants.URL_TEST_STRING);
		assertEquals("Dies ist eine Test URL , sie befindet sich in einem Satz", urlUDF.exec(input));
	}

}
