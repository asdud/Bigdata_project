package day0706.blacklist;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class BlackListSplitBolt extends BaseRichBolt {

	private  OutputCollector collector;
	
	public void execute(Tuple tuple) {
		//处理日志：1,201.105.101.102,http://mystore.jsp/?productid=1,2017020029,2,1
		String log = tuple.getString(0);
		
		//分词
		String[] words = log.split(",");
		
		//过滤掉，不满足要求的日志数据
		if(words.length == 6){
			//每个user_id记一次数
			this.collector.emit(new Values(Integer.parseInt(words[0]),1));
		}
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// 初始化
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declare) {
		// 申明schema的格式: (user_id,1)
		declare.declare(new Fields("userID","count"));
	}
}















