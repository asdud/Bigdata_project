package day0706.blacklist;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

//每10秒钟，统计过去30秒内访问频率超过4次的用户信息。
public class BlackListTotalByWindowBolt extends BaseWindowedBolt {

	private OutputCollector collector;
	
	//定义一个集合，来保存该窗口处理的结果
	private Map<Integer, Integer> result = new HashMap<Integer, Integer>();
	
	//参数：inputWindow该窗口中的数据
	public void execute(TupleWindow inputWindow) {
		//得到该窗口中的所有数据
		List<Tuple> list = inputWindow.get();
		
		//处理该窗口中的所有数据
		for(Tuple t:list){
			//取出数据
			int userID = t.getIntegerByField("userID");
			int count = t.getIntegerByField("count");
			
			//求和
			if(result.containsKey(userID)){
				//如果包含，累加
				int total = result.get(userID);
				result.put(userID, total+count);
			}else{
				//第一次出现
				result.put(userID, count);
			}
			
			//输出
			System.out.println("统计的结果: " + result);
		
			//频率超过4次的用户信息  -----> 输出MySQL数据库
			if(result.get(userID) > 4){
				//把这个用户信息发给下一个bolt组件 -----> 输出MySQL数据库
				this.collector.emit(new Values(userID,result.get(userID)));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("userid","PV"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
}












