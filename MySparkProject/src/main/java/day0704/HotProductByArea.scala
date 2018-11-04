package day0704
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

//地区表
case class AreaInfo(area_id:String,area_name:String)

//商品表 用不到的数据，不要导入
case class ProductInfo(product_id:String,product_name:String)

//经过清洗后的，用户点击日志信息
case class LogInfo(user_id:String,user_ip:String,product_id:String,click_time:String,action_type:String,area_id:String)

object HotProductByArea {
  def main(args:Array[String]):Unit={
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    val spark=SparkSession.builder().master("local").appName("").getOrCreate()
    import spark.sqlContext.implicits._
    
    //获取地区数据
    val areaDF = spark.sparkContext.textFile("hdfs://hdp21:8020/input/project04/area/areainfo.txt")
                 .map(_.split(",")).map(x=> new AreaInfo(x(0),x(1))).toDF()
    areaDF.createTempView("area")
    
    //获取商品数据
   val productDF = spark.sparkContext.textFile("hdfs://hdp21:8020/input/project04/product/productinfo.txt")
                 .map(_.split(",")).map(x=> new ProductInfo(x(0),x(1)))
                 .toDF()   
    productDF.createTempView("product")
                 
    //获取点击日志
    val clickLogDF = spark.sparkContext.textFile("hdfs://hdp21:8020/cleandata/project04")
                     .map(_.split(",")).map(x => new LogInfo(x(0),x(1),x(2).substring(x(2).indexOf("=")+1),x(3),x(4),x(5)))
                     .toDF()                 
    clickLogDF.createTempView("clicklog")   
    
    //执行SQL
    val sql = "select a.area_id,a.area_name,p.product_id,product_name,count(c.product_id) from area a,product p,clicklog c where a.area_id=c.area_id and p.product_id=c.product_id group by a.area_id,a.area_name,p.product_id,p.product_name"
  
    spark.sql(sql).show()
    
    spark.stop()
    
  }
}