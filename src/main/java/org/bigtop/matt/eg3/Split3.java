package org.bigtop.matt.eg3;

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
public class Split3 extends InputSplit implements Writable {
	
	private static int count = 1;

	public Split3() {
		System.out.println("MySplit constructor called: " + count);
		count++; 
	}

	/**
	 * Warning: dont expect this to be called at runtime, except when job is created.
	 * InputSplits are created via serialization loading, not via custom constructors.
	 */
	int records;
	int splits;
	public Split3(int records , int splits) {
        System.out.println("MySplit constructor called: " + count);
        this.records=records;
        this.splits=splits;
        count++;  
    }
	
	public void readFields(DataInput arg0) throws IOException {
	    records=arg0.readInt();
	    splits=arg0.readInt();
	}

	public void write(DataOutput arg0) throws IOException {
        arg0.writeInt(records);
        arg0.writeInt(splits);
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
