package day0706.hotip;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.shade.org.apache.http.conn.scheme.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import kafka.utils.Log4jController$;
import scala.Function0;
import scala.actors.threadpool.Arrays;
import scala.runtime.BoxedUnit;

public class HotIPMain {
	public static void main(String[] args) {
		//创建一个任务：Topology=spout+bolt
		//Spout 从Kafka中接收数据
		TopologyBuilder builder=new TopologyBuilder();
		
		//Spout 从Kafka中接收数据
		
		//指定任务的spout的组件，接收kafaka的数据
		String zks="192.168.234.21:2181";
		//topic的名字
		String topic="mytopic";
		
		String zkRoot="storm";
		
		String id="mytopic";
		
		//指定Broker地址信息		
		BrokerHosts hosts=new ZkHosts(zks);
		SpoutConfig spoutConf=new SpoutConfig(hosts, topic, zkRoot, id);
		//指定从kafka接收的是字符串
		spoutConf.scheme=new SchemeAsMultiScheme(new StringScheme());
		spoutConf.zkServers=Arrays.asList(new String[] {"192.168.234.21"});
		spoutConf.zkPort=2181;
		
		
		//指定任务的Spout组件，分词
		builder.setSpout("kafka_reader", new KafkaSpout(spoutConf));
		//指定任务的第一个bolt组件，分词
		builder.setBolt("split_bolt", new HotIPSplitBolt()).shuffleGrouping("kafka_reader");
		
		//指定任务的第二个bolt组件，用计数
		builder.setBolt("hotip_bolt",new HotIPTotalBolt()).fieldsGrouping("split_bolt", new Fields("ip"));
		
		Config Conf=new Config();
			LocalCluster cluster=new LocalCluster();
		
		
		//指定任务的第二个bolt组件，计数
	}

}
