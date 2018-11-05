package day0702

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.lang.Double

//定义case class代表Schema
//商品表
case class Product(prod_id:Int,prod_name:String)
//订单表
case class SaleOrder(prod_id:Int,year_id:Int,amount:Double)

object ProductSalesInfo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ProductSalesInfo").setMaster("local")
    
    //创建SparkSession，也可以直接创建SQLContext
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._    
    
    //第一步：取出商品的数据
    val productInfo = sc.textFile("hdfs://hdp21:8020/input/sh/products").map(line=>{
      val words = line.split(",")
      //返回： 商品的ID和商品的名称
      (Integer.parseInt(words(0)),words(1))
    }).map(d=>Product(d._1,d._2)).toDF()
        
    //第二步：取出订单的数据
    val orderInfo = sc.textFile("hdfs://hdp21:8020/input/sh/sales").map(line=>{
      val words = line.split(",")
      
      //返回： 商品的ID、年份、金额
      (Integer.parseInt(words(0)),
       Integer.parseInt(words(2).substring(0, 4)),    //1998-01-10
       Double.parseDouble(words(6))
       )
    }).map(d=>SaleOrder(d._1,d._2,d._3)).toDF()
    
    //注册视图
    productInfo.createTempView("product")
    orderInfo.createTempView("saleorder")
    
    //查询
    /*
	    select prod_name,year_id,sum(amount)
      from product,saleorder
      where product.prod_id=saleorder.prod_id
      group by prod_name,year_id
     */
    //第一步：得到结果
    var sql1 = "select prod_name,year_id,sum(amount) "
    sql1 = sql1 + " from product,saleorder "
    sql1 = sql1 + " where product.prod_id=saleorder.prod_id"
    sql1 = sql1 + " group by prod_name,year_id"
    
    val result = sqlContext.sql(sql1).toDF("prod_name","year_id","total")
    result.createTempView("result")
    
    //第二步：将行转成列
    sqlContext.sql("select prod_name,sum(case year_id when 1998 then total else 0 end),sum(case year_id when 1999 then total else 0 end),sum(case year_id when 2000 then total else 0 end),sum(case year_id when 2001 then total else 0 end) from result group by prod_name").show
    
    sc.stop()
  }
}





















