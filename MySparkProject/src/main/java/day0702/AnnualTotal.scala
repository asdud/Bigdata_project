package day0702

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.lang.Double

//定义一个case class来代表订单表
//指定DataFrame的schema
case class OrderInfo(year:Int,amount:Double)

object AnnualTotal {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AnnualTotal").setMaster("local")
    
    //创建SparkSession，也可以直接创建SQLContext
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    //从HDFS上读入数据
    val myData = sc.textFile("hdfs://hdp21:8020/input/sh/sales").map(
        //13,1660,1998-01-10,3,999,1,1232.16
       line => {
         val words = line.split(",")
         
         //取出 年份和金额
         (Integer.parseInt(words(2).substring(0,4)),Double.parseDouble(words(6)))
     }).map(d=>OrderInfo(d._1,d._2)).toDF()
     
     //myData.printSchema()
     
     //创建视图
     myData.createTempView("orders")
     
     //执行查询  1、SparkSession.sql()   2、sqlContext.sql()
     sqlContext.sql("select year,count(amount),sum(amount) from orders group by year").show()
     
     sc.stop()
    
  }
}














