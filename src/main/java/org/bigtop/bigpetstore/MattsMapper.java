package org.bigtop.bigpetstore;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class MattsMapper extends Mapper<Text, Text, Text, Text> {
    
    @Override
    protected void setup(Context c)  throws IOException,
            InterruptedException {
        super.setup(c);
        System.out.println("I'm a silly setup method, doing nothing (MattsMapper)");
    }

	@Override
	protected void map(Text key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		Text key2 = context.getCurrentKey();
		Text val2 = context.getCurrentValue();
		System.out.println("my silly string: " + key2 + " " + key2.toString() + " " + key2.getLength());
		context.write(key, value);
	};

}
