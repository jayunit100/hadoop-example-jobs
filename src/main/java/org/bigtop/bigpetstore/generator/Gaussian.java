package org.bigtop.bigpetstore.generator;

import java.util.Date;
import java.util.Random;
import static org.bigtop.bigpetstore.generator.SomeData.*;


public class Gaussian implements Generator {

	private final Random random;
	private int MAX_PRICE;
	
	public Gaussian() {
        this(0);
	}
	
	public Gaussian(int max_price) {
		this.random = new Random();
		this.MAX_PRICE = max_price;
	}
	
	private double getGaussian() {
		double val = this.random.nextGaussian() % 1;
		if(val > 1 || val < -1) {
			throw new RuntimeException("omg wtf? " + val);
		}
		return Math.abs(val);
	}
	
	/**
	 * I don't know if this works.  Does casting a double to an int
	 * round, truncate up, or truncate down?
	 */
	public String getFirstName() {
		double myRandom = getGaussian();
		double num = myRandom * FIRSTNAMES.length;
//		System.out.println("my first random stuff: " + myRandom + "  " + num + "  " + ((int) num));
		return FIRSTNAMES[(int) num];
	}

	public String getLastName() {
		double myRandom = getGaussian();
		double num = myRandom * LASTNAMES.length;
//		System.out.println("my last random stuff: " + myRandom + "  " + num + "  " + ((int) num));
		return LASTNAMES[(int) num];
	}

	public String getProduct() {
		double myRandom = getGaussian();
		double num = myRandom * PRODUCTS.length;
//		System.out.println("my random stuff: " + myRandom + "  " + num + "  " + ((int) num));
		return PRODUCTS[(int) num];
	}

	public Date getDate() {
		long date = ((Double) (getGaussian() * Long.MAX_VALUE)).longValue();
		return new Date(date);
	}

	public Integer getPrice() {
		int price = ((Double) (getGaussian() * MAX_PRICE)).intValue();
		return price;
	}

}
