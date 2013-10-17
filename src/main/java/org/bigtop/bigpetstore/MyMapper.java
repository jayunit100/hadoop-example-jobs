package org.bigtop.bigpetstore;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



class MyMapper extends
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

	@Override
	protected void map(Text key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		// added by Matt
//		context.getCurrentKey();
//		context.getCurrentValue();
		context.write(key, value);
	};
}
