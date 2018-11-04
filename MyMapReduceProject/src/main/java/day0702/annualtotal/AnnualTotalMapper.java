package day0702.annualtotal;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnnualTotalMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	
	@Override
	protected void map(LongWritable key2, Text value1,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String data=value1.toString();
		
		String[] words=data.split(",");
		
		context.write(new IntWritable(Integer.parseInt("1")), new DoubleWritable(value));
	}

	

}
