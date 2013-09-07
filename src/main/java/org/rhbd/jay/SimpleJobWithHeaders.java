package org.rhbd.jay;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

/**
 * Scientific data often comes with headers in files, unlike the quintessential
 * word count mapreduce example, where files are uniform.
 * 
 * When you need meta data in the header, you will want to make a record reader
 * which is "smart", i.e., which initializes by reading the first data lines,
 * and then starts to return key/values afterwards.
 * 
 */
public class SimpleJobWithHeaders extends Job {

	private void createInput(Configuration conf, Path inDir) throws IOException {
		Path infile = new Path(inDir, "text.txt");
		OutputStream os = FileSystem.get(conf).create(infile);
		Writer wr = new OutputStreamWriter(os);
		/***
		 * Consider a word count input file, where the first line of the file
		 * contains the words to use in the count.
		 * 
		 * ****************** COUNT=dog,cat My dog is not a cat my dog is not a
		 * giraffe my dog is my freind! ******************
		 * 
		 * The outputs of a word count for this job would be:
		 * 
		 * dog,3 cat,1
		 * 
		 * In order to implement this sort of wordount, we need non splittable
		 * inputs, that is, inputs which process headers independently.
		 */
		wr.write("dog,cat\n");
		wr.write("My dog is not a cat\n");
		wr.write("My dog is not a cat\n");
		wr.write("my dog is not a giraffe\n");
		wr.write("my dog is my freind!\n");
		wr.close();
		System.out.println("DONE WRITING " + infile);
	}

	/**
	 * This is the standard word count mapper class.
	 */
	public static class WordCountMapper extends
			Mapper<Text, Text, Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);

		protected void map(Text offset, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tok = new StringTokenizer(value.toString(), " ");
			while (tok.hasMoreTokens()) {
				Text word = new Text(tok.nextToken());
				context.write(word, ONE);
			}
		}
	}

	public static class HeaderInputFormat extends FileInputFormat<Text, Text> {
		Text header;
		Set<String> wordsToCount = new HashSet<String>();

		/**
		 * We dont split files when files have headers. We need to read the
		 * header in the init method, and then use that meta data in the header
		 * for counting all words in the file.
		 */
		@Override
		protected boolean isSplitable(JobContext context, Path file) {
			return false;
		}

		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit arg0,
				TaskAttemptContext arg1) throws IOException,
				InterruptedException {
			return new KeyValueLineRecordReader(arg1.getConfiguration()) {
				@Override
				public void initialize(InputSplit genericSplit,
						TaskAttemptContext context) throws IOException {
					super.initialize(genericSplit, context);
					/**
					 * Read the first line.
					 */
					super.nextKeyValue();
					header = super.getCurrentKey();
					System.out.println("header = " + header);
					if (header.toString().length() == 0)
						throw new RuntimeException("No header :(");
					String[] words = header.toString().split(",");
					for (String s : words) {
						wordsToCount.add(s);
						System.out.println("COUNTING " + s);
					}
					System.out.println("\nCOUNT ::: " + wordsToCount);
				}

				/**
				 * In this case, we'll just return the same thing for both the
				 * key and value. A little strange but.. its a good way to
				 * demonstrate that the RecordReader decouples us from the
				 * default implementation of key/value parsing... which is the
				 * point of this post anyways :)
				 */
				@Override
				public Text getCurrentKey() {
					// TODO Auto-generated method stub
					return this.getCurrentValue();
				}

				/**
				 * Starts at line 2. First line is the header.
				 */
				@Override
				public Text getCurrentValue() {
					String raw = super.getCurrentKey().toString();
					String[] fields = raw.split(" ");
					String filteredLine = "";
					/**
					 * Use the metadata in the header to filter out strings
					 * which we're not interested in.
					 */
					for (String f : fields) {
						if (wordsToCount.contains(f)) {
							filteredLine += f + " ";
						}
					}
					System.out
							.println("RECORD READER FILTERED LINE USING HEADER META DATA: "
									+ filteredLine);
					return new Text(filteredLine);
				}
			};
		}
	}

	public SimpleJobWithHeaders(Configuration conf) throws IOException {
		try {
			// these in/out dirs will exist in /mnt/glusterfs/
			Path inDir = new Path("testing/jobconf/input");
			Path outDir = new Path("testing/jobconf/output");

			FileSystem.get(this.getConfiguration()).delete(inDir, true);
			FileSystem.get(this.getConfiguration()).delete(outDir, true);

			/**
			 * Write the input file.
			 */
			createInput(this.getConfiguration(), inDir);

			setJobName("MapReduce example for scientific data with headers");

			setOutputKeyClass(Text.class);
			setOutputValueClass(IntWritable.class);

			setInputFormatClass(HeaderInputFormat.class);
			setMapperClass(WordCountMapper.class);
			setReducerClass(IntSumReducer.class);
			FileInputFormat.addInputPath(this, inDir);
			FileOutputFormat.setOutputPath(this, outDir);
			Assert.assertTrue(this.waitForCompletion(true));

			// Check the output is as expected
			FileStatus[] outputFiles = FileSystem.get(this.getConfiguration())
					.listStatus(outDir);
			for (FileStatus file : outputFiles) {
				System.out.println(file.getPath() + " " + file.getLen());
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}

	}

	public static void main(String[] args) {
		try {
			new SimpleJobWithHeaders(new Configuration());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}