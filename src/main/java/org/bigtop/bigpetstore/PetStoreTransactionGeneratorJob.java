package org.bigtop.bigpetstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bigtop.bigpetstore.Constants.STATE;
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
			final int MAX = 10;
			for (int i = 0; i < MAX; i++) {
				context.getCounter("test_counters_", i + "").increment(1);
			}
			//memory!
			context.getCounter("freemem", Runtime.getRuntime().freeMemory() + "").increment(1);
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
		
		@Override
		public RecordReader<Text, Text> createRecordReader(
				final InputSplit inputSplit, TaskAttemptContext arg1) throws IOException,
				InterruptedException {
				return new RecordReader<Text, Text>() {
				
				@Override
				public void close() throws IOException {

				}
				
				Text name, transaction;
				Random r = new Random();
				/**
				 * We need the "state" information to generate records.
				 * - Each state has a probability associated with it, so that
				 * our data set can be realistic (i.e. Colorado should have more transactions
				 * than rhode island).
				 * 
				 * - Each state also will its name as part of the key.
				 * 
				 * - This task would be distributed, for example, into 50 nodes
				 * on a real cluster, each creating the data for a given state.
				 */
				STATE stateOfTransactions = ((TransactionInputSplit) inputSplit).state;
				@Override
				public Text getCurrentKey() throws IOException,
						InterruptedException {
					return name;				
				}

				@Override
				public Text getCurrentValue() throws IOException,
						InterruptedException {
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
					name = new Text(
							FIRSTNAMES[r.nextInt(FIRSTNAMES.length - 1)]
									+ "_"
									+ LASTNAMES[r.nextInt(LASTNAMES.length - 1)]);
					transaction = new Text(
							PRODUCTS[r.nextInt(PRODUCTS.length - 1)]);
					
					//continue returning a new mock transaction
					//until we exceed the number of transactions.
					System.out.println(name + " " + transaction);
					return soFar++ < TRANSACTIONS;
				}

				@Override
				public float getProgress() throws IOException,
						InterruptedException {
					return (float) soFar / (float) TRANSACTIONS;
				}

			};
		}

		@Override
		public List<InputSplit> getSplits(JobContext arg) throws IOException {
			int transactions = arg.getConfiguration().
					getInt("transactions",-1);
			if(transactions == -1 ){
				throw new RuntimeException("# of Transactions not set in configuration object: " + arg.getConfiguration());
			}

			ArrayList<InputSplit> list = new ArrayList<InputSplit>();

			/**
			 * Generator class will take a state as input and generate all 
			 * the data for that state.
			 */
			for(STATE s : STATE.values())
			{
				list.add(new TransactionInputSplit(s));
			}
			return list;
		}

	}
	
	public static class TransactionInputSplit extends InputSplit implements Writable {
		public STATE state;
		public TransactionInputSplit(STATE s) {
			super();
			this.state=s;
		}

		/**
		 * These are just hints to the JobTracker, so not a huge 
		 * deal  (todo confirm).
		 */
		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[] {};
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return -1;
		}

		/**
		 * Why do inputsplits need these?
		 */
		public void readFields(DataInput arg0) throws IOException {
		}
		public void write(DataOutput arg0) throws IOException {
		}
	}
	
	public static Job createJob(Path output, Configuration conf) throws IOException {
		Job job = new Job(conf, "PetStoreTransaction_ETL_"+System.currentTimeMillis());
		//recursively delete the data set if it exists.
		FileSystem.get(conf).delete(output,true);
		job.setJarByClass(PetStoreTransactionGeneratorJob.class);
		job.setMapperClass(PetStoreTransactionGeneratorJob.Map.class);
		//use the default reducer
		//job.setReducerClass(PetStoreTransactionGeneratorJob.Red.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TransactionGeneratorInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, output);
		return job;
	}
	
	public static void main(String args[]) throws Exception {
		if(args.length != 1)
		{
			System.err.println("USAGE : [number of records] [output path]");
			System.exit(0);
		}
		else{
			createJob(new Path(args[0]), new Configuration()).waitForCompletion(true);
		}
	
	}

}