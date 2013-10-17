package org.bigtop.bigpetstore;

import org.apache.hadoop.conf.Configuration;

public class ExampleRunner {

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		Eg1 eg1 = new Eg1(conf);
		eg1.runJob();
	}

}
