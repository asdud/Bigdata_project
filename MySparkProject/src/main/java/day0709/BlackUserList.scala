package day0709

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

//定义两个case class
case class HotIP(user_id:Int,pv:Int)
//只用到user_id和user_name
case class UserInfo(user_id:Int,username:String)

object BlackUserList {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)   
    
    //创建一个SparkContext和StreamingContext
    val conf = new SparkConf().setAppName("BlackUserList").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //使用已经存在的SparkContext创建StreamingContext
    val ssc = new StreamingContext(sc,Seconds(10))
    
    //由于使用Spark SQL分析数据，创建SQLContext对象
    val sqlContext = new SQLContext(ssc.sparkContext)
    import sqlContext.implicits._
    
    //加载用户的信息
    val userInfo = sc.textFile("hdfs://hdp21:8020/input/userinfo.txt")
                     .map(_.split(","))
                     .map(x=> new UserInfo(x(0).toInt,x(1))).toDF
    userInfo.createOrReplaceTempView("userinfo")
    
    //从Kafka中接收点击日志，分析用户的PV
    val topics = Map("mytopic"->1)
    
    //创建Kafka的输入流，只能使用基于Receiver方式
    val kafkaStream = KafkaUtils.createStream(ssc, "192.168.157.21:2181", "mygroup", topics)
    
    //日志：1,201.105.101.102,http://mystore.jsp/?productid=1,2017020020,1,1
    val logRDD = kafkaStream.map(_._2)
    
    //统计用户的PV：窗口操作                                                                           每个用户的ID，记一次数
    val hot_user_id = logRDD.map(_.split(",")).map(x=>(x(0),1))       //窗口的长度           滑动距离
                            .reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(30),Seconds(10))
                            
    //过滤异常的用户信息：比如：访问频率大于5次的用户ID
    val result = hot_user_id.filter(x=> x._2 > 5)  //------> 得到这段时间内的黑名单
    
    //查询黑名单用户的信息
    result.foreachRDD( rdd =>{
        //将每条黑名单用户注册成一个表
      val hotUserTable = rdd.map(x=> new HotIP(x._1.toInt,x._2)).toDF
      hotUserTable.createOrReplaceTempView("hotip")
      
      //关联用户表，查询黑名单的信息
      sqlContext.sql("select userinfo.user_id,userinfo.username,hotip.pv from userinfo,hotip where userinfo.user_id=hotip.user_id").show()
    })
    
    ssc.start()
    ssc.awaitTermination()
  }
}



















