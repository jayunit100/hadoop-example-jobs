package org.bigtop.bigpetstore;

import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * What does an `InputSplit` actually do?
 * From the Javadocs, it looks like ... absolutely nothing.
 */
public class MySplit extends InputSplit {
	
	private static int count = 1;

	public MySplit() {
		System.out.println("MySplit constructor called: " + count);
		count++; // is this synchronized?  who knows ....
	}
	
	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		System.out.println("getLocations in MySplit");
		return new String[] {};
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		System.out.println("getLength in MySplit");
		return 100;
	}
}
