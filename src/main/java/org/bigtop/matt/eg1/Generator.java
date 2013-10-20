package org.bigtop.matt.eg1;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.bigtop.bigpetstore.PetStoreTransactionGeneratorJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * A simple input split that fakes input.
 */
public class Generator extends
		FileInputFormat<Text,Text> {
	
	public Generator() {
		System.out.println("calling default Generator constructor");
	}
	
	final static Logger log = LoggerFactory
			.getLogger(PetStoreTransactionGeneratorJob.class);

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
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		System.out.println("creating record reader");
		return new RecordReader<Text, Text>() {
			
			@Override
			public void close() throws IOException {

			}
			
			Text name, transaction;
			Random r = new Random();

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
				System.out.println("in the generator: " + name + " " + transaction);
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
		int transactionsPerSplit = arg.getConfiguration().getInt("transactions",10);
		int splits = arg.getConfiguration().getInt("transaction_files",2);
		
		List<InputSplit> l = Lists.newArrayList();
		for(int i = 0 ; i < splits ; i++){
			log.info("generator log: " + i + " Adding a new input split of size " + transactionsPerSplit);
			l.add(new MySplit(/*transactionsPerSplit*/));
		}
		System.out.println("splits done");
		return l;
	}

}