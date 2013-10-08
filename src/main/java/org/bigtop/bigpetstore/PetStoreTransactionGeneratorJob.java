package org.bigtop.bigpetstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.runner.JUnitCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

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
 * each record 
 */
public class PetStoreTransactionGeneratorJob {

	final static Logger log = LoggerFactory
			.getLogger(PetStoreTransactionGeneratorJob.class);

	private static class Map extends
			Mapper<Text, Text, Text, Text> {

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			//For pipelines that might have lots of counters,
			//replace with 1000 to ensure that the counters limit
			//is large enough.
			for (int i = 0; i < 10; i++) {
				context.getCounter("test_counters_", i + "").increment(1);
			}
			//memory!
			context.getCounter("freemem", Runtime.getRuntime().freeMemory()+"").increment(1);
			context.getCounter("username", System.getProperty("user.name")).increment(1);

		}

		protected void map(Text key, Text value, Context context)
				throws java.io.IOException, InterruptedException {
			context.write(key, value);
		};
	}

	/**
	 * A simple input split that fakes input.
	 */
	public static class TransactionGeneratorInputFormat extends
			FileInputFormat<Text,Text> {
		static final Integer TRANSACTIONS = 100;

		static String[] FIRSTNAMES = new String[]{
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
		static String[] LASTNAMES = new String[]{
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
		static String[] PRODUCTS = new String[]{
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
		
		@Override
		public RecordReader<Text, Text> createRecordReader(
				InputSplit arg0, TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return new RecordReader() {
				@Override
				public void close() throws IOException {

				}
				Text name, transaction;
				Random r = new Random();
				@Override
				public Text getCurrentKey() throws IOException,
						InterruptedException {
					// TODO Auto-generated method stub
					return name;				
				}

				@Override
				public Text getCurrentValue() throws IOException,
						InterruptedException {
					// TODO Auto-generated method stub
					return transaction;
				}

				@Override
				public void initialize(InputSplit arg0, TaskAttemptContext arg1)
						throws IOException, InterruptedException {
				}
				int soFar=0;
				@Override
				public boolean nextKeyValue() throws IOException,
						InterruptedException {
					name=new Text(FIRSTNAMES[r.nextInt(FIRSTNAMES.length-1)]+"_"+LASTNAMES[r.nextInt(LASTNAMES.length-1)]);
					transaction=new Text(PRODUCTS[r.nextInt(PRODUCTS.length-1)]);
					
					//continue returning a new mock transaction
					//until we exceed the number of transactions.
					System.out.println(name + " " + transaction);
					return soFar++ < TRANSACTIONS;
				}

				@Override
				public float getProgress() throws IOException,
						InterruptedException {
					// TODO Auto-generated method stub
					return (float)soFar/(float)TRANSACTIONS;
				}

				
			};
		}

		@Override
		public List<InputSplit> getSplits(JobContext arg) throws IOException {
			int transactionsPerSplit = arg.getConfiguration().getInt("transactions",10);
			int splits = arg.getConfiguration().getInt("transaction_files",2);
			
			List<InputSplit> l = Lists.newArrayList();
			for(int i = 0 ; i < splits ; i++){
				log.info(i+ " Adding a new input split of size " + transactionsPerSplit);
				l.add(new PetStoreTransactionGeneratorJob.TransactionInputSplit(/*transactionsPerSplit*/));
			}
			return l;
		}

	}
	public static class TransactionInputSplit extends InputSplit implements Writable {

		public void readFields(DataInput arg0) throws IOException {
		}

		public void write(DataOutput arg0) throws IOException {
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[] {};
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return 100;
		}
}
	
	public Job getJob(String[] args, Configuration conf) throws IOException {
		Job job = new Job(conf, "Dud Job");

		FileSystem.get(conf).delete(new Path("dud"));
		job.setJarByClass(PetStoreTransactionGeneratorJob.class);
		job.setMapperClass(PetStoreTransactionGeneratorJob.Map.class);
		//job.setReducerClass(PetStoreTransactionGeneratorJob.Red.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TransactionGeneratorInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("dud"));
		return job;
	}

	public static void main(String args[]) throws Exception {
		new PetStoreTransactionGeneratorJob().getJob(args, new Configuration()).waitForCompletion(true);
	}

}