package org.bigtop.bigpetstore;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Eg1 {
	
	private final Job job;
	
	public Eg1(Configuration conf) throws IOException {
		System.out.println("Eg1 default constructor called");
		job = new Job(conf, "Dud Job");

		FileSystem.get(conf).delete(new Path("dud"));
		job.setJarByClass(Eg1.class);
		job.setMapperClass(MyMapper.class);
		//job.setReducerClass(PetStoreTransactionGeneratorJob.Red.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(Generator.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("dud"));
	}
	
	public void runJob() throws Exception {
		this.job.waitForCompletion(true);
	}

}