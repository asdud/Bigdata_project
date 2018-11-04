package day0713

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix


object UserBasedCF {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    //创建一个SparkContext
    val conf=new SparkConf().setAppName("BlackUserList").setMaster("local")
     val sc=new SparkContext(conf)
    
    //读入数据
    val data=sc.textFile("D:\\download\\data\\ratingdata.txt")
    
    
    val parseData:RDD[MatrixEntry]=data.map(_.split(",")
        match{case Array(user,item,rate)=>MatrixEntry(user.toLong,item.toLong,rate.toDouble)})
        
     //构造评分矩阵
    val ratings=new CoordinateMatrix(parseData)
    
    val matrix:RowMatrix=ratings.transpose().toRowMatrix()
    
    val similarities=matrix.columnSimilarities()
    println("输出用户相似度矩阵")
    similarities.entries.collect().map(x=>{
      println(x.i+"--->"+x.j+"--->"+x.value)
    })
    
  }
}