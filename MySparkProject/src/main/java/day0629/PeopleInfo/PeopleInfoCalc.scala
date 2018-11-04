package day0629.PeopleInfo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PeopleInfoCalc {
  def main(args:Array[String]): Unit={
    //创建SparkContext
    val conf=new SparkConf.setAppName("PeopleInfoCalc").setMaster("local")
    val sc=new SparkContext(conf)
    
    //读取数据
    val dataFile=sc.textFile("")
    //拆分：男。女
    val maleData=dataFile.filter(f)
    val femaleData=dataFile.filter(f)
    
    val maleHeightData=
    val femaleHeightData=
    
    val lowestMale=maleHeightData
    val lowestFemale=femaleHeightData.so
    
    
  }
}