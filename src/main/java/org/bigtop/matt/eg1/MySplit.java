package org.bigtop.matt.eg1;

import java.io.IOException;

import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * What does an `InputSplit` actually do?
 * From the Javadocs, it looks like ... absolutely nothing.
 *
 * Note: for some reason, you *have* to implement Writable,
 * even if your methods do nothing, or you will got strange
 * and un-debuggable null pointer exceptions.
 */
public class MySplit extends InputSplit implements Writable {
	
	private static int count = 1;

	public MySplit() {
		System.out.println("MySplit constructor called: " + count);
		count++; // is this synchronized?  who knows ....
	}
	
	public void readFields(DataInput arg0) throws IOException {
//		throw new RuntimeException("unimplemented method MySplitter.readFields");
		System.out.println("calling MySplitter.readFields ... for some reason");
	}

	public void write(DataOutput arg0) throws IOException {
//		throw new RuntimeException("unimplemented method MySplitter.write");
		System.out.println("calling MySplitter.write ... for some reason");
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
