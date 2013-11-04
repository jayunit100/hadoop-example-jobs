package org.bigtop.bigpetstore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.bigtop.bigpetstore.Constants.STATE;



/**
 * A simple input split that fakes input.
 */
public class Format extends
		FileInputFormat<Text,Text> {

	@Override
	public RecordReader<Text, Text> createRecordReader(
			final InputSplit inputSplit, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		return new RecordReader<Text, Text>() {
			
			@Override
			public void close() throws IOException {

			}
			
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
					
			String storeCode = ((Split) inputSplit).storeCode;
			int records = ((Split) inputSplit).numRecords;
			Iterator<KeyVal<String, String>> data = (new Data(storeCode, records)).getData().iterator();
			KeyVal<String, String> currentRecord;

			@Override
			public Text getCurrentKey() throws IOException,
					InterruptedException {
				return new Text(currentRecord.key);
			}

			@Override
			public Text getCurrentValue() throws IOException,
					InterruptedException {
				return new Text(currentRecord.val);
			}

			@Override
			public void initialize(InputSplit arg0, TaskAttemptContext arg1)
					throws IOException, InterruptedException {
			}
			
			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				if(data.hasNext()) {
					currentRecord = data.next();
					return true;
				}
				return false;
			}

			@Override
			public float getProgress() throws IOException,
					InterruptedException {
				return 0f;
			}

		};
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg) throws IOException {
		int totalRecords = arg.getConfiguration().
				getInt("totalRecords",-1);
		if(totalRecords == -1 ){
			throw new RuntimeException("# of total records not set in configuration object: " + 
		        arg.getConfiguration());
		}

		ArrayList<InputSplit> list = new ArrayList<InputSplit>();

		/**
		 * Generator class will take a state as input and generate all 
		 * the data for that state.
		 */
		for(STATE s : STATE.values())
		{
			System.out.println("about to create Split");
			Split split = new Split();
			System.out.println("setting store code for " + split.my_id);
			split.storeCode = s.name();
			System.out.println("setting number of records");
			split.numRecords = (int) (totalRecords * s.probability);
			System.out.println(split.storeCode + " records : " + split.numRecords);
			list.add(split);
		}
		return list;
	}

}
