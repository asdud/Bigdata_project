package day0702.annualtotal;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//                                                                k2年份                     v2 金额
public class AnnualTotalMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	@Override
	protected void map(LongWritable key1, Text value1,Context context)
			throws IOException, InterruptedException {
		//数据：13,1660,1998-01-10,3,999,1,1232.16
		String data = value1.toString();
		//分词
		String[] words = data.split(",");
		
		//输出  k2年份                     v2 金额
		context.write(new IntWritable(Integer.parseInt(words[2].substring(0, 4))), 
		              new DoubleWritable(Double.parseDouble(words[6])));
	}

}
