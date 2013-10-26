package org.bigtop.matt.eg3;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;


public class Data3 {

	private final List<KeyVal<String, String>> data;
	private final Random r;

	public Data3(String state) {
		this.data = new ArrayList<KeyVal<String, String>>();
		this.r = new Random();
		int SIZE = 100;
		String key, val;
		for(int trans_id = 1; trans_id <= SIZE; trans_id++) {
			key = join(",", "BigPetStore", state, trans_id + "");
			val = join(",",
					   this.getLastName(),
					   this.getFirstName(),
					   this.getDate().toString(),
					   (this.getPrice() / 100f) + "",
					   this.getProduct());
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
			temp += "," + strs[i];
		}
		return temp;
	}
	
	public List<KeyVal<String, String>> getData() {
		return this.data;
	}
	
	private String getFirstName() {
		return FIRSTNAMES[this.r.nextInt(FIRSTNAMES.length - 1)];
	}
	
	private String getLastName() {
		return LASTNAMES[this.r.nextInt(LASTNAMES.length - 1)];
	}
	
	private String getProduct() {
		return PRODUCTS[this.r.nextInt(PRODUCTS.length - 1)];
	}
	
	private Date getDate() {
		return new Date(this.r.nextInt());
	}
	
	private Integer getPrice() {
		return this.r.nextInt(MAX_PRICE);
	}

	static final Integer MINUTES_IN_DAY = 60 * 24;
	static final Integer MAX_PRICE = 10000;
	
	static final String[] FIRSTNAMES = new String[]{
		"jay",
		"john",
		"jim",
		"diana",
		"duane",
		"david",
		"peter",
		"paul",
		"matthias",
		"hyacinth",
		"jacob",
		"andrew",
		"andy",
		"mischa",
		"enno",
		"sanford",
		"shawn"
	};
	static final String[] LASTNAMES = new String[]{
		"vyas",
		"macleroy",
		"watt",
		"childress",
		"shaposhnick",
		"bukatov",
		"govind",
		"jones",
		"stevens",
		"yang",
		"fu",
		"ghandi",
		"watson",
		"walbright",
		"samuelson"
	};
	static final String[] PRODUCTS = new String[]{
		"dog food",
		"cat food",
		"fish food",
		"little chew toy",
		"big chew toy",
		"dog treats (hard)",
		"dog treats (soft)",
		"premium dog food",
		"premium cat food",
		"pet deterrent",
		"flea collar",
		"turtle food",
	};

}
