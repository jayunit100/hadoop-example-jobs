package org.bigtop.bigpetstore;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

public class MattsGenerator extends InputFormat<Text, Text> {

	@Override
	public List<InputSplit> getSplits(JobContext arg) throws IOException {
		List<InputSplit> l = Lists.newArrayList();
		return l;
	}
	
	@Override
	public RecordReader<Text, Text> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new RecordReader<Text, Text>() {
			
			@Override
			public void close() throws IOException {
                System.out.println("calling no-op RecordReader.close stub");
			}
			
			Text um1, um2;
			int count = 0;

			@Override
			public Text getCurrentKey() throws IOException,	InterruptedException {
				return um1;
			}

			@Override
			public Text getCurrentValue() throws IOException, InterruptedException {
				return um2;
			}

			@Override
			public void initialize(InputSplit arg0, TaskAttemptContext arg1)
					throws IOException, InterruptedException {
				System.out.println("now RecorderReader.initialize no-op");
			}
			
			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				boolean done = !(count < 1);
				count++;
				return done;
			}

			@Override
			public float getProgress() {
				return 0;
			}

		};
	}

}
