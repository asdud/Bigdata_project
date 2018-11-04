package day0629.peopleinfo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PeopleInfoMain {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(PeopleInfoMain.class);
		
		job.setMapperClass(PeopleMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PeopleInfo.class);

		job.setReducerClass(PeopleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}
