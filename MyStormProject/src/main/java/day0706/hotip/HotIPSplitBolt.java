package day0706.hotip;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.mysql.fabric.xmlrpc.base.Value;

public class HotIPSplitBolt extends BaseRichBolt{
	
	private OutputCollector collector;

	public void execute(Tuple tuple) {
		// 处理日志:
		String log=tuple.getString(0);
		
		//分词
		String[] words=log.split(",");
		
		//过滤掉不满足要求的日志数据
		if (words.length==6) {
			this.collector.emit(new Values(words[1],1));
		}
 	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector=collector;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// T
		declarer.declare(new Fields("ip","count"));
	}

}
