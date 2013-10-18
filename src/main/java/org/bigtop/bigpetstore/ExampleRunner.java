package org.bigtop.bigpetstore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ExampleRunner {

	public static void main(String args[]) throws Exception {
		System.out.println(args + " " + args.length);
		eg1();
//		eg2();
	}
	
	public static void eg1() throws Exception {
		Configuration conf = new Configuration();
		
		MySetup set = new MySetup(conf, 
				MyMapper.class, 
				Text.class, 
				Text.class, 
				Text.class, 
				Text.class, 
				Generator.class,
				TextOutputFormat.class);

		set.runJob();
	}
	
	public static void eg2() throws Exception {
		Configuration conf = new Configuration();
		
		MySetup set = new MySetup(conf, 
				MattsMapper.class, 
				Text.class, 
				Text.class, 
				Text.class, 
				Text.class, 
				MattsGenerator.class,
				TextOutputFormat.class);

		set.runJob();
	}

}
