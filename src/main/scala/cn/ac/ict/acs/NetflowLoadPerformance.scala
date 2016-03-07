import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by arvin on 16-3-1.
 */

case class Netflow(
                 srcAddr: String,
                   dstAddr: String,
                   nexthop: String,
                   input: Long,
                   output: Long,
                   dPkts: Long,
                   dOctets: Long,
                   first: Long,
                   last: Long,
                   srcport: Int,
                   destPort: Int,
                   tcp_flags: Int,
                   prot: Int,
                   tos: Int,
                   src_as: Long,
                   dst_as: Long,
                   src_mask: Int,
                   dst_mask: Int
                   )
// case class fields greater than 22, shuold use custom subclass of product.
// 3.2 Arvin
//val BigNetflow = CustomCaseClass(
//                   nfVersion : Int,
//                   flowSetCount: Int,
//                   sysUptime: Int,
//                   epochTime: Int,
//                   nanoSeconds: Int,
//                   flowsSeen: Int,
//                   engineType: Int,
//                   engineId: Int,
//                   samplingInfo: Int,
//
//                   srcAddr: String,
//                   dstAddr: String,
//                   nexthop: String,
//                   input: Long,
//                   output: Long,
//                   dPkts: Long,
//                   dOctets: Long,
//                   first: Long,
//                   last: Long,
//                   srcport: Int,
//                   destPort: Int,
//                   tcp_flags: Int,
//                   prot: Int,
//                   tos: Int,
//                   src_as: Long,
//                   dst_as: Long,
//                   src_mask: Int,
//                   dst_mask: Int
//                   )
object NetflowLoadPerformance{
  val INPUT_DIR = "/netflow"
  val OUTPUT_DIR: String = "/netflow"

  // get the name of the class excluding dollar signs and package
  val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")

  // create spark context and set class name as the app name
  val sc = new SparkContext(new SparkConf().setAppName("Netflow " + className))

  // convert an RDDs to a DataFrames
  val sqlContext = new HiveContext(sc)

  import sqlContext.implicits._

  def bigNetflow = sc.textFile(INPUT_DIR + "/bigNetflow5").map(_.split(' ')).map(p => CustomCaseClass(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt,
    p(4).trim.toInt, p(5).trim.toInt, p(6).trim.toInt, p(7).trim.toInt, p(8).trim.toInt, p(9).trim, p(10).trim, p(11).trim, p(12).trim.toLong, p(13).trim.toLong,
    p(14).trim.toLong, p(15).trim.toLong, p(16).trim.toLong, p(17).trim.toLong, p(18).trim.toInt, p(19).trim.toInt, p(20).trim.toInt, p(21).trim.toInt,
    p(22).trim.toInt, p(23).trim.toLong, p(24).trim.toLong, p(25).trim.toInt, p(26).trim.toInt)).toDF()

  def netflow = sc.textFile(INPUT_DIR + "/netflow5").map(_.split(' ')).map(p => Netflow(p(0).trim, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim.toLong,
    p(5).trim.toLong, p(6).trim.toLong, p(7).trim.toLong, p(8).trim.toLong, p(9).trim.toInt, p(10).trim.toInt, p(11).trim.toInt, p(12).trim.toInt,
    p(13).trim.toInt, p(14).trim.toLong, p(15).trim.toLong, p(16).trim.toInt, p(17).trim.toInt)).toDF()

  def outputDF(df: DataFrame, format: String): Unit ={
    val startTime = System.currentTimeMillis()

    if(format == "orc"){
      df.write.mode("overwrite").orc(OUTPUT_DIR + "/" + className + ".orc")
    }
    else if(format == "parquet"){
      df.write.mode("overwrite").parquet(OUTPUT_DIR + "/" + className + ".parquet")
    }
    val endTime = System.currentTimeMillis()
    println("write " + format + "'s time used is " + (endTime - startTime))
  }


  // to turn on snappy.
  def execute(): Unit ={
    sc.hadoopConfiguration.set("hive.exec.orc.default.compress","SNAPPY")
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
  }


  def main(args: Array[String]): Unit = {
    require(args.length > 2)


    val tblName = args(0)
    // orc or parquet
    val flag = args(1)
    // on or off
    val snappyFlag = args(2)

    if(snappyFlag == "on")
      execute()

    tblName match {
      case "bigNetflow" =>
        outputDF(bigNetflow,flag)
      case "netflow" =>
        outputDF(netflow,flag)
      case _ =>
        println("There doesn't exist this table name!")

    }


  }


}