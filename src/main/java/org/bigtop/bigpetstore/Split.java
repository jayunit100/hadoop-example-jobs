package org.bigtop.bigpetstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;



public class Split extends InputSplit implements Writable {
	public   String storeCode;
	public   int numRecords;
    public int my_id;
	
	private static int split_id = 1;
	
	public Split() {
		this.my_id = split_id;
		split_id++;
	}
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
	
	public void write(DataOutput arg0) throws IOException {
//		arg0.writeInt(numRecords);
//		arg0.write(storeCode.getBytes());
		arg0.writeUTF(this.numRecords + "," + this.storeCode);
	}

	/**
	 * Why do inputsplits need these?
	 */
	public void readFields(DataInput arg0) throws IOException {
//		this.numRecords = arg0.readInt();
//		this.storeCode = arg0.readLine();
		String s = arg0.readUTF();
		String [] fs = s.split(",");
		System.out.println("got: " + s);
		this.numRecords = Integer.parseInt(fs[0]);
		this.storeCode = fs[1];
	}
}
