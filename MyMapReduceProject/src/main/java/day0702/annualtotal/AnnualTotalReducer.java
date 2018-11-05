package day0702.annualtotal;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//                                                                            k4 年份           v4 订单数+销售总额
public class AnnualTotalReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, Text> {

	@Override
	protected void reduce(IntWritable k3, Iterable<DoubleWritable> v3,Context context)
			throws IOException, InterruptedException {
		// 将同一年的金额和个数求和
		double totalCount = 0;
		double totalMoney = 0;
		for(DoubleWritable v:v3){
			totalCount ++;
			totalMoney += v.get();
		}
		
		// k4 年份           v4 订单数+销售总额
		context.write(k3, new Text(totalCount+"\t"+totalMoney));
	}

}
