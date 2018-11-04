package day0629.sogou;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SogouLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key1, Text value1, Context context)
			throws IOException, InterruptedException {
		//数据 20111230000005  
		String data = value1.toString();
		
		String[] words = data.split("\t");
		
		//数据清洗
		if(words.length != 6) return;
		
		try{
			//找到URL排名第一、用户排名顺序第二的
			if(Integer.parseInt(words[3]) == 1 && Integer.parseInt(words[4]) == 2){
				context.write(value1, NullWritable.get());
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
}
