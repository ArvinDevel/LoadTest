/**
 * Created by arvin on 16-2-26.
 */

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
case class Customer(
                     c_custkey: Int,
                     c_name: String,
                     c_address: String,
                     c_nationkey: Int,
                     c_phone: String,
                     c_acctbal: Double,
                     c_mktsegment: String,
                     c_comment: String)

case class Lineitem(
                     l_orderkey: Int,
                     l_partkey: Int,
                     l_suppkey: Int,
                     l_linenumber: Int,
                     l_quantity: Double,
                     l_extendedprice: Double,
                     l_discount: Double,
                     l_tax: Double,
                     l_returnflag: String,
                     l_linestatus: String,
                     l_shipdate: String,
                     l_commitdate: String,
                     l_receiptdate: String,
                     l_shipinstruct: String,
                     l_shipmode: String,
                     l_comment: String)

case class Nation(
                   n_nationkey: Int,
                   n_name: String,
                   n_regionkey: Int,
                   n_comment: String)

case class Order(
                  o_orderkey: Int,
                  o_custkey: Int,
                  o_orderstatus: String,
                  o_totalprice: Double,
                  o_orderdate: String,
                  o_orderpriority: String,
                  o_clerk: String,
                  o_shippriority: Int,
                  o_comment: String)

case class Part(
                 p_partkey: Int,
                 p_name: String,
                 p_mfgr: String,
                 p_brand: String,
                 p_type: String,
                 p_size: Int,
                 p_container: String,
                 p_retailprice: Double,
                 p_comment: String)

case class Partsupp(
                     ps_partkey: Int,
                     ps_suppkey: Int,
                     ps_availqty: Int,
                     ps_supplycost: Double,
                     ps_comment: String)

case class Region(
                   r_regionkey: Int,
                   r_name: String,
                   r_comment: String)

case class Supplier(
                     s_suppkey: Int,
                     s_name: String,
                     s_address: String,
                     s_nationkey: Int,
                     s_phone: String,
                     s_acctbal: Double,
                     s_comment: String)



abstract class TpchLoadPerformance {

  // read files from local FS
  // val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/dbgen"

  // read from hdfs
  val INPUT_DIR: String = "/dbgen"

  // if set write results to hdfs, if null write to stdout
  val OUTPUT_DIR: String = "/tpch"

  // get the name of the class excluding dollar signs and package
  val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")

  // create spark context and set class name as the app name
  val sc = new SparkContext(new SparkConf().setAppName("TPC-H " + className))

  // convert an RDDs to a DataFrames
  val sqlContext = new HiveContext(sc)

  import sqlContext.implicits._

  def customer = sc.textFile(INPUT_DIR + "/customer.tbl").map(_.split('|')).map(p => Customer(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF()
  def lineitem = sc.textFile(INPUT_DIR + "/lineitem.tbl").map(_.split('|')).map(p => Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()
  def nation = sc.textFile(INPUT_DIR + "/nation.tbl").map(_.split('|')).map(p => Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF()
  def region = sc.textFile(INPUT_DIR + "/region.tbl").map(_.split('|')).map(p => Region(p(0).trim.toInt, p(1).trim, p(1).trim)).toDF()
  def order = sc.textFile(INPUT_DIR + "/orders.tbl").map(_.split('|')).map(p => Order(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)).toDF()
  def part = sc.textFile(INPUT_DIR + "/part.tbl").map(_.split('|')).map(p => Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()
  def partsupp = sc.textFile(INPUT_DIR + "/partsupp.tbl").map(_.split('|')).map(p => Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim)).toDF()
  def supplier = sc.textFile(INPUT_DIR + "/supplier.tbl").map(_.split('|')).map(p => Supplier(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(): Unit

  def outputDF(df: DataFrame): Unit = {

    if (OUTPUT_DIR == null || OUTPUT_DIR == "")
      df.collect().foreach(println)
    else
      df.write.mode("overwrite").json(OUTPUT_DIR + "/" + className + ".out") // json to avoid alias
  }
  def outputDF(df: DataFrame, format: String): Unit ={


    val startTime = System.currentTimeMillis()

    if(format == "orc"){
      df.write.mode("overwrite").orc(OUTPUT_DIR + "/" + df.toString() + ".orc")
    }
    else if(format == "parquet"){
      df.write.mode("overwrite").parquet(OUTPUT_DIR + "/" + df.toString() + ".parquet")
    }
    val endTime = System.currentTimeMillis()
    println("write " + format + "'s time used is " + (endTime - startTime))
  }
}

object TpchLoadPerformance extends TpchLoadPerformance{


  def main(args: Array[String]): Unit = {
    require(args.length > 2)


    val tblName = args(0)
    // orc or parquet
    val flag = args(1)
    // on or off
    val snappyFlag = args(2)

    // use snappy or uncompressed or default
    if(snappyFlag == "snappy")
      execute()
    else if(snappyFlag == "uncompressed")
      uncompressed()


    tblName match {
      case "customer" =>
        outputDF(customer,flag)
      case "lineitem" =>
        outputDF(lineitem,flag)
      case "nation" =>
        outputDF(nation,flag)
      case "region" =>
        outputDF(region,flag)
      case "order" =>
        outputDF(order,flag)
      case "part" =>
        outputDF(part,flag)
      case "partsupp" =>
        outputDF(partsupp,flag)
      case "supplier" =>
        outputDF(supplier,flag)
      case _ =>
        println("There doesn't exist this table name!")

    }


  }

  // to turn on snappy.
  // 2.26 Arvin
  override def execute(): Unit ={
    sc.hadoopConfiguration.set("hive.exec.orc.default.compress","SNAPPY")
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
  }

  // should test uncompressed performance
  // 3.11 Arvin
  def uncompressed(): Unit ={
    sc.hadoopConfiguration.set("hive.exec.orc.default.compress","NONE")
    sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")
  }
}
