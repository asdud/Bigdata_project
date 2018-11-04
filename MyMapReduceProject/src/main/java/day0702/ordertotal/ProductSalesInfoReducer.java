package day0702.ordertotal;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductSalesInfoReducer extends Reducer<IntWritable, Text, Text, Text> {
@Override
protected void reduce(IntWritable k3, Iterable<Text> v3, Context context)
		throws IOException, InterruptedException {
	String productName="";
	
	Map<Integer, Double> result= new HashMap<Integer, Double>();
	
	
	for (Text v : v3) {
		String str=v.toString();
		int index=str.indexOf("name:");
		if (index>=0) {
			productName=str.substring(5);
		} else {
         int year=Integer.parseInt(str.substring(0, 4));
         double amount=Double.parseDouble(str.substring(str.lastIndexOf(":")+1));
		
		if (result.containsKey(year)) {
			result.put(year, result.get(year)+amount);
		}else {
			result.put(year, amount);
		}
		}
		
	}
	context.write(new Text(productName), new Text(result.toString()));

}
}
