package day0716

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD

/*
 * 1、如何代表人工标注的数据？
 *     case class LabeledPoint(label: Double, features: Vector)
 *     参数：label: 人工标注的数据
 *          features: 特征向量
 *          
 * 2、如何进行逻辑回归？
 * 		LogisticRegressionWithLBFGS: 支持多分类，可以更快的聚合（速度快、效果好）
 *    LogisticRegressionWithSGD  : 梯度下降法，只支持二分类。在Spark 2.1后，基本被废弃了
 *    
 */
object PredictProduct {
  def main(args: Array[String]): Unit = {
    //设置环境变量
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)   
    
    val conf = new SparkConf().setMaster("local").setAppName("PredictProduct")
    val sc = new SparkContext(conf)
    
    //读入数据 ----> 封装LabeledPoint
    val data = sc.textFile("D:\\download\\data\\sales.data")
    val parseData = data.filter(_.length() > 0).map(line =>{
        //分词操作：按照 @分词
        val parts = line.split("@")
        //生成一个LabeledPoint(label: Double, features: Vector)
        //             人工标注的数据                                       该行的特征向量
        LabeledPoint(parts(0).trim().toDouble,Vectors.dense(parts(1).split(",").map(_.trim().toDouble)))     
    })
    
    //执行逻辑回归                                                                  2表示是二分类
    //val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(parseData)
    val model = new LogisticRegressionWithSGD().run(parseData)
    
    //生产上，将模型保存到HDFS
    //model.save(sc, "hdfs://192.168.157.21:8020/PredictModel")
    
    //模型已经存在
    //val model = LogisticRegressionModel.load(sc, "hdfs://192.168.157.21:8020/PredictModel")
    
    
    //使用该模型进行预测：
    /* 用户的特征数据：
     * 1,23,175,6  ------>  买还是不买？1.0
     * 0,22,160,5  ------>  买还是不买？0.0
     */
    val target = Vectors.dense(1,23,175,6)
    //val target = Vectors.dense(0,22,160,5)
    
    //执行预测
    val result = model.predict(target)
    println("买还是不买？" + result)
    
    sc.stop()
  }
}















