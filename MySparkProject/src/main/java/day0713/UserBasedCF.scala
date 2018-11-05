package day0713

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix


/*
 * 1、建立用户的相似度矩阵
 * 2、计算一些数据
 */
object UserBasedCF {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)   
    
    //创建一个SparkContext
    val conf = new SparkConf().setAppName("BlackUserList").setMaster("local")
    val sc = new SparkContext(conf)
    
    //读入数据
    val data = sc.textFile("D:\\download\\data\\ratingdata.txt")
    //MatrixEntry代表：矩阵中的一行
    //使用模式匹配
    val parseData:RDD[MatrixEntry] = data.map(_.split(",") 
        match {case Array(user,item,rate) => MatrixEntry(user.toLong,item.toLong,rate.toDouble)} )
        
    //构造评分矩阵 new CoordinateMatrix(entries: RDD[MatrixEntry]) 
    val ratings = new CoordinateMatrix(parseData)
    
    //计算用户的相似度矩阵:需要进行矩阵的转置
    val matrix:RowMatrix = ratings.transpose().toRowMatrix()
    
    //进行计算，得到了用户的相似度矩阵
    val similarities = matrix.columnSimilarities()
    println("输出用户相似度矩阵")
    similarities.entries.collect().map(x=>{
      println(x.i +" --->" + x.j+ "  ----> " + x.value)
    })
    
    println("-----------------------------------------")
    //得到某个用户对所有物品的评分：用户1为例
    val ratingOfUser1 = ratings.entries.filter(_.i == 1).map(x=>(x.j,x.value)).sortBy(_._1).collect().map(_._2).toList.toArray
    println("用户1对所有物品的评分")
    for(s<-ratingOfUser1) println(s)
    
    println("-----------------------------------------")
    
    //得到用户1相对于其他用户的相似性
    val similarityOfUser1 = similarities.entries.filter(_.i == 1).sortBy(_.value,false).map(_.value).collect
    println("用户1相对于其他用户的相似性")
    for(s<- similarityOfUser1) println(s)
      
    
    sc.stop()
  }
}



















