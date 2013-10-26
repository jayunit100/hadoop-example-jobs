package org.bigtop.matt.eg3;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;


public class Gen3 extends InputFormat<Text, Text> {

	@Override
	public RecordReader<Text, Text> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new RecordReader<Text, Text>() {
			
			private Iterator<KeyVal<String, String>> data = (new Data3("CT")).getData().iterator();
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

	@Override
	public List<InputSplit> getSplits(JobContext arg) throws IOException {
		List<InputSplit> l = Lists.newArrayList();
//		for(int i = 0; i < 10; i++) {
			l.add(new Split3());
//		}
		return l;
	}
	
}
