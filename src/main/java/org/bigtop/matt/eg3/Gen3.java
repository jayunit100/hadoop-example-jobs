package org.bigtop.matt.eg3;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mrunit.types.Pair;

import com.google.common.collect.Lists;


public class Gen3 extends InputFormat<Text, Text> {

	@Override
	public RecordReader<Text, Text> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
	    this.readSplitConfigs(arg1.getConfiguration());
		return new RecordReader<Text, Text>() {
			
			private Iterator<KeyVal<String, String>> data = (new Data3(records,"CT")).getData().iterator();
			private KeyVal<String, String> currentRecord;
			
			@Override
			public void close() throws IOException {
                System.out.println("calling no-op RecordReader.close stub");
			}
			
			@Override
			public Text getCurrentKey() throws IOException,	InterruptedException {
				return new Text(currentRecord.key);
			}

			@Override
			public Text getCurrentValue() throws IOException, InterruptedException {
				return new Text(currentRecord.val);
			}

			@Override
			public void initialize(InputSplit arg0, TaskAttemptContext arg1)
					throws IOException, InterruptedException {
				System.out.println("now RecorderReader.initialize no-op");
			}
			
			/*
			 * note to self: `nextKeyValue` should 
			 *  return true when *not* done, return false when done
             */
			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				if(this.data.hasNext()) {
					this.currentRecord = this.data.next();
					return true;
				}
				return false;
			}

			@Override
			public float getProgress() {
				// TODO
				return 0f;
			}

		};
	}

	public enum props {
	    bigpetstore_splits,
	    bigpetstore_records_per_split
	}

	int splits ; int records;
	public void readSplitConfigs(Configuration conf){
	     splits = conf.getInt(props.bigpetstore_splits.name(),0);
	     if(splits ==0 ){
	         throw new RuntimeException("missing "+props.bigpetstore_splits);
	     }
	     records = conf.getInt(props.bigpetstore_records_per_split.name(),0);
	     if(records ==0 ){
             throw new RuntimeException("missing "+props.bigpetstore_records_per_split);
         }
	}
	@Override
	public List<InputSplit> getSplits(JobContext arg) throws IOException {
		readSplitConfigs(arg.getConfiguration()); // sets splits/records
		List<InputSplit> l = Lists.newArrayList();
        
		for(int i = 0; i < splits; i++) {
			l.add(new Split3(splits, records));
		}
		return l;
	}
	
}
