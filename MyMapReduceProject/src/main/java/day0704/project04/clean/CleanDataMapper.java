package day0704.project04.clean;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanDataMapper  extends 	Mapper<LongWritable, Text, Text, NullWritable>{

	@Override
	protected void map(LongWritable key1, Text value1,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String log=value1.toString();
		String [] words=log.split(",");
		
		if (words.length==6&&words[2].startsWith("http")) {
			context.write(value1, NullWritable.get());
		}
	}

	
}
