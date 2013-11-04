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
	}

	protected void map(Text key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		context.write(key, value);
	};
}