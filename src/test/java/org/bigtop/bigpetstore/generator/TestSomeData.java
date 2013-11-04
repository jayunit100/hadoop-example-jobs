package org.bigtop.bigpetstore.generator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestSomeData {

	@Test
    public void testLastNames() {
        assertEquals(SomeData.LASTNAMES.length, 15);
    }
	
	@Test
    public void testFirstNames() {
        assertEquals(SomeData.FIRSTNAMES.length, 17);
    }
	
	@Test
    public void testProducts() {
        assertEquals(SomeData.PRODUCTS.length, 12);
    }
	
}
