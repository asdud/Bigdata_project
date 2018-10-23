package day0629

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PeopleInfoCalc {
  def main(args: Array[String]): Unit = {
    //创建SparkContext
    //val conf = new SparkConf().setAppName("PeopleInfoCalc").setMaster("local")
    val conf = new SparkConf().setAppName("PeopleInfoCalc")
    val sc = new SparkContext(conf)
    
    //读取数据
    //val dataFile = sc.textFile("d:\\temp\\sample_people_info.txt")
    val dataFile = sc.textFile(args(0))
    
    //拆分：男、女                                                                                                     数据     2 M 105             性别                                                      升高
    val maleData = dataFile.filter(line=>line.contains("M")).map(line=>(line.split(" ")(1) + " " + line.split(" ")(2)))
    val feMaleData = dataFile.filter(line=>line.contains("F")).map(line=>(line.split(" ")(1) + " " + line.split(" ")(2)))
    
    //得到各自的身高
    val maleHeighData = maleData.map(line=>line.split(" ")(1).toInt)
    val femaleHeighData = feMaleData.map(line=>line.split(" ")(1).toInt)
    
    //最高和最低值
    val lowestMale = maleHeighData.sortBy(x=>x, true).first()
    val lowestFeMale = femaleHeighData.sortBy(x=>x, true).first() 
    
    val highesttMale = maleHeighData.sortBy(x=>x, false).first()
    val highestFeMale = femaleHeighData.sortBy(x=>x, false).first() 
    
    //输出
    println("lowestMale:" + lowestMale)
    println("lowestFeMale:" + lowestFeMale)
    println("highesttMale:" + highesttMale)
    println("highestFeMale:" + highestFeMale)
    
    sc.stop()
  }
}

















