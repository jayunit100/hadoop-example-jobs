package org.bigtop.matt.eg2;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mrunit.types.Pair;
import org.bigtop.matt.eg1.MySplit;

import com.google.common.collect.Lists;

public class Gen2 extends InputFormat<Text, Text> {

	@Override
	public RecordReader<Text, Text> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new RecordReader<Text, Text>() {
			
			private List<Pair<String, String>> data = (new Data2()).getData();
			
			@Override
			public void close() throws IOException {
                System.out.println("calling no-op RecordReader.close stub");
			}
			
			int index = 0;

			@Override
			public Text getCurrentKey() throws IOException,	InterruptedException {
				return new Text(data.get(index - 1).getFirst());
			}

			@Override
			public Text getCurrentValue() throws IOException, InterruptedException {
				return new Text(data.get(index - 1).getSecond());
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
				boolean notDone = index < data.size();
				index++;
				return notDone;
			}

			@Override
			public float getProgress() {
				// uh ... I hope this gives the right float.
				// can't really remember java's rules of coercion
				return ((float) index) / data.size();
			}

		};
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg) throws IOException {
		List<InputSplit> l = Lists.newArrayList();
//		for(int i = 0; i < 10; i++) {
			l.add(new MySplit());
//		}
		return l;
	}
	
}
