package day0704.project04.clean;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
过滤不满足6个字段的数据
过滤URL为空的数据，即：过滤出包含http开头的日志记录
 */
public class CleanDataMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key1, Text value1, Context context)
			throws IOException, InterruptedException {
		String log = value1.toString();
		
		//分词
		String[] words = log.split(",");
		
		if(words.length == 6 && words[2].startsWith("http")){
			context.write(value1, NullWritable.get());
		}
	}

}
