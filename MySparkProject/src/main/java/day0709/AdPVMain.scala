package demo.project07

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

object AdPVMain {
  //定义case class保存地区信息和广告点击日志信息
  //地区表
  case class AreaInfo(area_id:Int,area_name:String)
  
  //广告点击日志
  //1,201.105.101.102,2017020029,http://ad1.jsp/?key=1,2
  case class AdLogInfo(userid:Int,ip:String,clickTime:String,url:String,area_id:Int)
    
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    //设置环境变量
    System.setProperty("hadoop.home.dir", "D:\\tools\\hadoop-2.4.1")
    System.setProperty("HADOOP_USER_NAME","hdfs")
    
    //创建SparkSession
    val spark = SparkSession.builder().master("local").appName("Project07-AdPVMain").getOrCreate()
    import spark.sqlContext.implicits._
    
    val areaInfoDF=spark.sparkContext.textFile("")
    .map(_.split(",")).map(x=>new AreaInfo(x(0).toInt,x(1),x(2),x(3),x(4).toInt)).toDF
        

  }
}


















