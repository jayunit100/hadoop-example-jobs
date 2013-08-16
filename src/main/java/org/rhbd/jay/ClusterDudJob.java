package org.rhbd.jay;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;

/**
 * This is a "dud" job
 * 
 */
public class ClusterDudJob  {

	final static Logger log = LoggerFactory
			.getLogger(ClusterDudJob.class);

	private static class Map extends
			Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			for (int i = 0; i < 1000; i++) {
				context.getCounter("test_counters_", i + "").increment(1);
			}
			//memory!
			context.getCounter("freemem", Runtime.getRuntime().freeMemory()+"").increment(1);
		}

		protected void map(NullWritable key, NullWritable value, Context context)
				throws java.io.IOException, InterruptedException {

		};
	}

	/**
	 * A simple input split that fakes input.
	 */
	public static class EmptyInputFormat extends
			FileInputFormat<NullWritable, NullWritable> {

		@Override
		public RecordReader<NullWritable, NullWritable> createRecordReader(
				InputSplit arg0, TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return new RecordReader() {
				@Override
				public void close() throws IOException {

				}

				@Override
				public Object getCurrentKey() throws IOException,
						InterruptedException {
					// TODO Auto-generated method stub
					return NullWritable.get();
				}

				@Override
				public Object getCurrentValue() throws IOException,
						InterruptedException {
					// TODO Auto-generated method stub
					return NullWritable.get();
				}

				@Override
				public float getProgress() throws IOException,
						InterruptedException {
					// TODO Auto-generated method stub
					return 0;
				}

				@Override
				public void initialize(InputSplit arg0, TaskAttemptContext arg1)
						throws IOException, InterruptedException {
					// TODO Auto-generated method stub

				}

				boolean reck = true, reckv = true;

				@Override
				public boolean nextKeyValue() throws IOException,
						InterruptedException {
					reck = false;
					return reck;
				}

			};
		}

		@Override
		public List<InputSplit> getSplits(JobContext arg0) throws IOException {
			// TODO Auto-generated method stub
			CustomInputSplit ci = new CustomInputSplit();
			log.info("Returning a single input");

			List<InputSplit> l = Lists.newArrayList();

			l.add(new DudInputSplit());
			return l;

		}

	}

	private static class Red
			extends
			Reducer<NullWritable, NullWritable, NullWritable, NullWritable> {
		@SuppressWarnings({ "rawtypes", "unused" })
		protected void setup(Context context) throws java.io.IOException,
				InterruptedException {
			log.info("Just having a coffee."); // to test logging framework
			log.info("Asserting some jars " + Tuple.class.getName()); // to test
																		// logging
																		// framework
			log.info("Asserting some jars " + JUnit4.class.getCanonicalName()); // to
																				// test
																				// logging
																				// framework
			// any other weird tests worth doing?
		};
	}

	public Job getJob(String[] args, Configuration conf) throws IOException {
		Job job = new Job(conf, "Dud Job");

		HadoopUtils.deleteIfExists(FileSystem.get(conf), new Path(
				"dud"));
		job.setJarByClass(ClusterDudJob.class);
		job.setMapperClass(ClusterDudJob.Map.class);
		job.setReducerClass(ClusterDudJob.Red.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setInputFormatClass(EmptyInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/"));
		FileOutputFormat.setOutputPath(job, new Path("dud"));
		return job;
	}

	public static void main(String args[]) throws Exception {
		new ClusterDudJob().getJob(args, new Configuration()).waitForCompletion(true);
	}

	static class DudInputSplit extends InputSplit implements Writable {

		public DudInputSplit() {
			super();
			// TODO Auto-generated constructor stub
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return new String[] { "/" };
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}
	};
}