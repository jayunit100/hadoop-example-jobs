package org.bigtop.bigpetstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;



public class Split extends InputSplit implements Writable {
	public   String storeCode;
	public   int numRecords;

	/**
	 * These are just hints to the JobTracker, so not a huge 
	 * deal  (todo confirm).
	 */
	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[] {};
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return -1;
	}

	/**
	 * Why do inputsplits need these?
	 */
	public void readFields(DataInput arg0) throws IOException {
		this.numRecords = arg0.readInt();
		this.storeCode = arg0.readLine();
	}
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(numRecords);
		arg0.write(storeCode.getBytes());
	}
}
