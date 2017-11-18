package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	protected void setup(
			Context context)
			throws java.io.IOException, InterruptedException {
	};

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws java.io.IOException, InterruptedException {
		
		//key -- > 0
		//value --> DEER RIVER RIVER
		
		final long inputOffset = key.get();
		final String inputLine = value.toString();
		
		if(StringUtils.isNotEmpty(inputLine))
		{
			final String[] words = StringUtils.splitPreserveAllTokens(inputLine, " ");
			
			
			Text output = new Text();
			final IntWritable ONE = new IntWritable(1);
			
			for (String word : words) {
				output.set(word);
				context.write(output, ONE );
			}
		
		}
		
		
	};

	protected void cleanup(
			Context context)
			throws java.io.IOException, InterruptedException {
	};

}
