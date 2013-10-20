package org.bigtop.matt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bigtop.matt.eg1.Generator;
import org.bigtop.matt.eg1.MyMapper;
import org.bigtop.matt.eg2.Gen2;
import org.bigtop.matt.eg2.Map2;

public class ExampleRunner {

	public static void main(String args[]) throws Exception {
		System.out.println(args + " " + args.length);
//		eg1();
		eg2();
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
				Map2.class, 
				Text.class, 
				Text.class, 
				Text.class, 
				Text.class, 
				Gen2.class,
				TextOutputFormat.class);

		set.runJob();
	}

}
