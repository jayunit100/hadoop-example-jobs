package org.bigtop.matt.eg3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class Map3 extends Mapper<Text, Text, Text, Text> {
    
    @Override
    protected void setup(Context c)  throws IOException,
            InterruptedException {
        super.setup(c);
        System.out.println("setup method, doing nothing (Map3)");
    }

	@Override
	protected void map(Text key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		Text key2 = context.getCurrentKey();
		Text val2 = context.getCurrentValue();
		System.out.println("Map3 string: " + key2 + " " + key2.toString() + " " + key2.getLength());
		System.out.println("Map3 string: " + val2 + " " + val2.toString() + " " + val2.getLength());

		context.write(key, value);
	};

}
