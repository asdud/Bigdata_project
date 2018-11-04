package day0629.sogou;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SogouLogMain {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//1，创建job
		Job job = Job.getInstance(conf);
		job.setJarByClass(SogouLogMain.class);
		
		//2.制定任务的Mapper和输出的类型
		job.setMapperClass(SogouLogMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
        
		//3.制定任务的Reducer和输出的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		//4.任务的输入和输出
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//5.执行
		job.waitForCompletion(true);
	}

}
