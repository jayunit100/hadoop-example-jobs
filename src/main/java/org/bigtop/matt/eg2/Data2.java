package org.bigtop.matt.eg2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.mrunit.types.Pair;

public class Data2 {

	private final List<Pair<String, String>> data = new ArrayList<Pair<String,String>>();
	
	public Data2() {
	    Random r = new Random();
	    for(char c: "abcdefghijklmnopqrstuvwxyz".toCharArray()) {
	    	String s = "";
	    	for(int i = 0; i < 10; i++) {
	    		s += r.nextInt(10000) + ",";
	    	}
	    	data.add(new Pair<String, String>(c + "", s));
	    }
	}
	
	public List<Pair<String, String>> getData() {
		return data;
	}
}
