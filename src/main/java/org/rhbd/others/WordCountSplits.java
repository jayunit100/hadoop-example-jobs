package org.rhbd.others;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.io.Files;

public class WordCountSplits {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err
					.println("Usage: wordcount <in|bytes> <out> <minsplit> <maxsplit> ---");
			System.err
					.println("The first arg, if a number, will create a file of X bytes. ");
			System.err
					.println("Example: wordcount 1000000 mbout 10000 10000 : Create a 1MB file with 10KB splits and process");
			System.exit(2);
		}
		
		Path toProcess = createInputPath(args, conf);
		
		Job job = createJob(conf, 
				toProcess, 
				Integer.parseInt(args[2]),
				Integer.parseInt(args[3]),
				new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * Either use args[0] or else create a file if args[0] is a number
	 */
	private static Path createInputPath(String[] args, Configuration conf)
			throws IOException {
		Path toProcess;
		{
			toProcess=new Path(args[0]);
			//mutate indfs in here.
			if (Character.isDigit(args[0].charAt(0))) {
				File inputfile=writeLocalFileOfSize(Integer.parseInt(args[0]));
				toProcess = new Path("/tmp/localinput"+System.currentTimeMillis());
				System.out.println("Copying " + inputfile.length() + " byte file to FS "+toProcess);
				FileSystem.get(conf).copyFromLocalFile(new Path(inputfile.getPath()), toProcess);
				System.out.println("File copied: length in dfs: " + 
						FileSystem.get(conf).getFileStatus(toProcess).getLen());
			}
		}
		return toProcess;
	}

	private static Job createJob(Configuration conf, Path input, int min, int max, Path out)
			throws IOException {
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCountSplits.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, input);

		FileInputFormat.setMaxInputSplitSize(job, min);
		FileInputFormat.setMaxInputSplitSize(job, max);

		FileOutputFormat.setOutputPath(job, out);
		return job;
	}

	private static File writeLocalFileOfSize(Integer bytes) throws IOException {
		File f = new File("/tmp/wcin"+System.currentTimeMillis()+".txt");
		
		while (f.length() < bytes) {
			Files.write(("a".getBytes() + "\n").getBytes(), f);
		}
		return f;
	}
}
