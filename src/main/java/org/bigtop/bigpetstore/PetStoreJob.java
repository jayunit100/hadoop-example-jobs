package org.bigtop.bigpetstore;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a mapreduce implementation of a generator of a large
 * sentiment analysis data set.  The scenario is as follows: 
 * 
 * An international pet store has archived 10s,100s, or 1000s, millions, or more 
 * of transactional data.  Some of the data includes unstructured text where 
 * a customer may have left a comment.  
 * 
 * Purpose: Generate a mock data set of "pet store" transactions.
 * Input: The number of "transactions" to generate. 
 * Output: A series of text files, with one record per line, where 
 * each record ???????????
 */
public class PetStoreJob {

	final static Logger log = LoggerFactory
			.getLogger(PetStoreJob.class);

	public static Job createJob(Path output, Configuration conf) throws IOException {
		Job job = new Job(conf, "PetStoreTransaction_ETL_"+System.currentTimeMillis());
		//recursively delete the data set if it exists.
		FileSystem.get(conf).delete(output,true);
		job.setJarByClass(PetStoreJob.class);
		job.setMapperClass(MyMapper.class);
		//use the default reducer
		//job.setReducerClass(PetStoreTransactionGeneratorJob.Red.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(Format.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		return job;
	}
	
	public static void alternateMain() throws Exception {
		/**
  	 	 * Part one: ETL of large, semistructured data into the
  	 	 * cluster.  
		 */
		Configuration c = new Configuration();
		c.setInt("totalRecords", 100);
		Job createInput = PetStoreJob.
				createJob(new Path("petstoredata/" + (new Date()).toString()), c);
		createInput.waitForCompletion(true);

		/**
		 * Part two: Basic analytics using pig and hive. 
		 */
		
	}

	public static void main(String args[]) throws Exception {
		final boolean dontRunAlternateMain = false;
		if(dontRunAlternateMain) {
			if(args.length != 2)
			{
				System.err.println("USAGE : [number of records] [output path]");
				System.exit(0);
			}
			else {
				Configuration conf = new Configuration();
				conf.setInt("totalRecords", Integer.parseInt(args[0]));
				createJob(new Path(args[1]), conf).waitForCompletion(true);
			}
		} else {
			alternateMain();
		}
	}
	
}