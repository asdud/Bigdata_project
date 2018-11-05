package day0706

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.rdd.RDD



//使用Spark SQL分析流式数据
//日志数据: 1,201.105.101.102,http://mystore.jsp/?productid=1,2017020029,2,1
case class LogInfo(user_id:String,user_ip:String,url:String,click_time:String,action_type:String,area_id:String)

object HotIP {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)   
    
    //创建一个StreamingContext
    val conf = new SparkConf().setAppName("HotIP").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(10))
    
    //由于使用Spark SQL分析数据，创建SQLContext对象
    val sqlContext = new SQLContext(ssc.sparkContext)
    import sqlContext.implicits._
    
    //指定Kafka的Topic，表示从这个topic中，每次接受一条数据
    val topic = Map("mytopic"->1)
    
    //创建DStream接受数据
    val kafkaStream = KafkaUtils.createStream(ssc,"192.168.157.21:2181","mygroup",topic)
    //从kafka中，接收到的数据是<key value>   key----> null空值
    val logRDD = kafkaStream.map(_._2)  //取出value
    
    //日志：1,201.105.101.102,http://mystore.jsp/?productid=1,2017020029,2,1
    logRDD.foreachRDD((rdd:RDD[String]) =>{
      //生成DataFrame
      val result = rdd.map(_.split(",")).map(x=> new LogInfo(x(0),x(1),x(2),x(3),x(4),x(5))).toDF()
      
      //生成视图
      result.createOrReplaceTempView("clicklog")
      //执行SQL
      sqlContext.sql("select user_ip as IP, count(user_ip) as PV from clicklog group by user_ip").show
    })
    
    ssc.start()
    ssc.awaitTermination()
  }
}



















