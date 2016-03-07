import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by arvin on 16-2-17.
 */
object ParquetWritePerformance {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("Spark Pi")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val startTime = System.currentTimeMillis()
    for (i <- 1 to 100) {
      val df = sc.parallelize(1 to 50000000).toDF()
      //write failure,change permmsion to make it.
      df.write.format("parquet") saveAsTable ("parquetFileTe"  + i)
    }
    val endTime = System.currentTimeMillis()
    println("the parquet time used is " + (endTime - startTime))

    sc.stop()

  }


}

