import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by arvin on 16-2-22.
 */

// the orc and parquet argument test
object WritePerformanceLocal{
  def main(args: Array[String]): Unit ={
   method2()
  }
  // Note that, spark configuration is just put in a map.

  // 1. sparkConf.set
  def method1(): Unit ={
    val sparkConf = new SparkConf().setAppName("DfTest")
    sparkConf.setMaster("local")
    sparkConf.set("spark.sql.tungsten.enabled","false")
    val sc = new SparkContext(sparkConf)

    // NoSuchElementException: parquet.compression.codec
    println(sparkConf.get("parquet.compression.codec"))

  }
  // 2. through sparkSql set/getConf
  def method2(): Unit ={
    val sparkConf = new SparkConf().setAppName("DfTest")
    sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    //    sqlContext.setConf("spark.sql.parquet.blocksize","1000")
    println(sqlContext.getConf("spark.sql.parquet.blocksize"))
    val compression = sqlContext.getConf("spark.sql.parquet.compression.codec")
    println("spark.sql.parquet.compression.codec is " + compression)


  }
  // 3. through hadoopConfiguration , if the name is consistent with the orc/paruqet, that will has effect.
  def method3(): Unit ={
    val sparkConf = new SparkConf().setAppName("DfTest")
    sparkConf.setMaster("local")

    val sc = new SparkContext(sparkConf)

    sc.hadoopConfiguration.set("parquet.blocksize","10")

    println(sc.hadoopConfiguration.get("parquet.blocksize"))

  }

}
