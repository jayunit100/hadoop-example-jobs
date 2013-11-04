package org.bigtop.bigpetstore.generator;

import java.util.Date;
import java.util.Random;
import static org.bigtop.bigpetstore.generator.SomeData.*;


public class Unbiased implements Generator {

	private final Random random;
	private final int MAX_PRICE;
	
	public Unbiased(Random r) {
		this(r, 10000);
	}
	
	public Unbiased(Random r, int MAX_PRICE) {
		this.random = r;
		this.MAX_PRICE = MAX_PRICE;
	}

	public String getFirstName() {
		return FIRSTNAMES[this.random.nextInt(FIRSTNAMES.length - 1)];
	}

	public String getLastName() {
		return LASTNAMES[this.random.nextInt(LASTNAMES.length - 1)];
	}

	public String getProduct() {
		return PRODUCTS[this.random.nextInt(PRODUCTS.length - 1)];
	}

	public Date getDate() {
		return new Date(this.random.nextInt());
	}

	public Integer getPrice() {
		return this.random.nextInt(MAX_PRICE);
	}

}
