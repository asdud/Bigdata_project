package day0702.ordertotal;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductSalesInfoMapper  extends Mapper<LongWritable, Text, IntWritable, Text>{
@Override
protected void map(LongWritable key1, Text value1,Context context)
		throws IOException, InterruptedException {
	// 输入的数据：订单、商品
	//使用判断文件名的方式
	//得到输入的HDFS的路径：--------> /input/sh/sales
	String path=((FileSplit)context.getInputSplit()).getPath().getName();
			String fileName=path.substring(beginIndex, endIndex);
			String data=value1.toString();
			String[] words=data.split(",");
			
			if(fileName.equals("products")) {
				context.write(new IntWritable(value), new Text(words[1]));
			}else {
				context.write(new IntWritable(Integer.parseInt(words[0])), arg1);
			}
	
}
}
