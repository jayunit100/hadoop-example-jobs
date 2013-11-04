package org.bigtop.bigpetstore;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigtop.bigpetstore.generator.Gaussian;
import org.bigtop.bigpetstore.generator.Generator;
import org.bigtop.bigpetstore.generator.Unbiased;


public class Data {

	private final List<KeyVal<String, String>> data;

	public Data(String state, int size) {
		this.data = new ArrayList<KeyVal<String, String>>();
		String key, val;
//		Generator g = new Unbiased(new Random(), 10000);
		Generator g = new Gaussian(10000);
		for(int trans_id = 1; trans_id <= size; trans_id++) {
			key = join(",", "BigPetStore", state, trans_id + "");
			val = join(",",
					   g.getLastName(),
					   g.getFirstName(),
					   g.getDate().toString(),
					   (g.getPrice() / 100f) + "",
					   g.getProduct());
			this.data.add(new KeyVal<String, String>(key, val));
		}
	}
	
	static String join(String sep, String ... strs) {
		if(strs.length == 0) {
			return "";
		} else if (strs.length == 1) {
			return strs[0];
		}
		String temp = strs[0]; // inefficient ... should probably use StringBuilder instead
		for(int i = 1; i < strs.length; i++) {
			temp += sep + strs[i];
		}
		return temp;
	}
	
	public List<KeyVal<String, String>> getData() {
		return this.data;
	}
	
}
