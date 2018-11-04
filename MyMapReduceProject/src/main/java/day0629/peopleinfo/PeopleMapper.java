package day0629.peopleinfo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PeopleMapper extends Mapper<LongWritable, Text, Text, PeopleInfo> {

	
	@Override
	protected void map(LongWritable key21, Text value1, Context context)
			throws IOException, InterruptedException {
		String data=value1.toString();
		String[] words=data.split(" ");
		
		PeopleInfo info=new PeopleInfo();
		info.setPeopleID(Integer.parseInt(words[0]));
		info.setGender(words[1]);
		info.setHeight(Integer.parseInt(words[2]));
		
		context.write(new Text(info.getGender()), info);
	}
}
