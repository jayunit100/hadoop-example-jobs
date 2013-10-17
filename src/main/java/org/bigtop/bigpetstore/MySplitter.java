package org.bigtop.bigpetstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class MySplitter extends InputSplit implements Writable {
	
	private static int count = 1;

	public MySplitter() {
		System.out.println("MySplitter constructor called: " + count);
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
		return new String[] {};
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return 100;
	}
}
