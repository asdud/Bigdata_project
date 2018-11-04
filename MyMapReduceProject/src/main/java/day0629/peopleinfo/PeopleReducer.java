package day0629.peopleinfo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PeopleReducer extends Reducer<Text, PeopleInfo, Text, IntWritable> {

	@Override
	protected void reduce(Text keys, Iterable<PeopleInfo> value3,
			Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int totalNumber=0;
		int highest=0;
		int lowest=10000;
		
		for (PeopleInfo p : value3) {
			totalNumber++;
			if (p.getHeight()>highest) {
				highest=p.getHeight();
			}
			
			if (p.getHeight()<lowest) {
				highest=p.getHeight();
			}
			
			context.write(new Text("Total: "), new IntWritable(totalNumber));
			context.write(new Text("Total: "), new IntWritable(highest));
			context.write(new Text("Total: "), new IntWritable(lowest));
		}
	}

	

}
