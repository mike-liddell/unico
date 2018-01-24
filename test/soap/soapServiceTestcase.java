package soap;

import junit.framework.TestCase;

public class soapServiceTestcase extends TestCase {

	public void testgcd() {
		assertEquals( 5, SOAPservice.calcGcdValue(25, 10));
		assertEquals( 33, SOAPservice.calcGcdValue(99, 66));
	}
}
