package day0706.hotip;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import scala.actors.threadpool.Arrays;

public class HotIPMain {

	public static void main(String[] args) {
		// 创建一个任务：Topology = spout + bolt
		//Spout 从Kafka中接收数据
		TopologyBuilder builder = new TopologyBuilder();
				
		//指定任务的spout的组件，接收kafka的数据
		//指定ZK的地址
		String zks = "192.168.157.21:2181";
		//topic的名字
		String topic = "mytopic";
		//Storm在ZK的根目录
		String zkRoot = "/storm";
		String id = "mytopic";
		//指定Broker地址信息
		BrokerHosts hosts = new ZkHosts(zks);		
		
		SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());  //指定从Kafka中接收的是字符串
		spoutConf.zkServers = Arrays.asList(new String[]{"192.168.157.21"});
		spoutConf.zkPort = 2181;
		builder.setSpout("kafka_reader", new KafkaSpout(spoutConf));
		
		//指定任务的第一个bolt组件，分词
		builder.setBolt("split_bolt", new HotIPSplitBolt()).shuffleGrouping("kafka_reader");
		
		//指定任务的第二个bolt组件，计数
		builder.setBolt("hotip_bolt", new HotIPTotalBolt()).fieldsGrouping("split_bolt", new Fields("ip"));

		//本地运行程序
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("MyHotIP", conf, builder.createTopology());
	}
}












