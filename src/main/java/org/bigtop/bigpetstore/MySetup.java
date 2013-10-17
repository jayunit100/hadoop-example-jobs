package org.bigtop.bigpetstore;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MySetup {
	
	private final Job job;
		
	// I don't know if the warnings the Class<? extends ...> causes are important or not
	public MySetup(Configuration conf,
			       Class<? extends Mapper> mapperClass,
			       Class<?> outkeyClass,
			       Class<?> outvalClass,
			       Class<?> mapOutKey,
			       Class<?> mapOutVal,
			       Class<? extends InputFormat> inClass,
			       Class<? extends OutputFormat> outClass) throws IOException {
		System.out.println("MySetup constructor called");
		this.job = new Job(conf, "Dud Job");

		FileSystem.get(conf).delete(new Path("dud"));
		
		job.setJarByClass(MySetup.class); // what on earth does this do, and how do I know if it's right or wrong?
		
		job.setMapperClass(MyMapper.class);
		//job.setReducerClass(PetStoreTransactionGeneratorJob.Red.class);
		
		job.setOutputKeyClass(outkeyClass);
		job.setOutputValueClass(outvalClass);
		job.setMapOutputKeyClass(mapOutKey);
		job.setMapOutputValueClass(mapOutVal);
		
		job.setInputFormatClass(inClass);
		job.setOutputFormatClass(outClass);
		
		FileOutputFormat.setOutputPath(job, new Path("dud"));
	}
	
	public void runJob() throws Exception {
		this.job.waitForCompletion(true);
	}

}