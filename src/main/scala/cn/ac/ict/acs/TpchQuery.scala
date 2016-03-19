import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by arvin on 16-3-14.
 */

// test the query performance on orc or parquet file, that's the file source is orc or parquet format,
// and the out file format should be consistent.
// TODO: orc or parquet tuning strategy is turning on?
// 3.14 Arvin
abstract class TpchQuery {

  // read files from local FS
  // val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/dbgen"

  // read from hdfs
  val INPUT_DIR: String = "/tbl"

  // if set write results to hdfs, if null write to stdout
  val OUTPUT_DIR: String = "/out"

  // get the name of the class excluding dollar signs and package
  val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")

  // create spark context and set class name as the app name
  val sc = new SparkContext(new SparkConf().setAppName("TPC-H " + className))

  // convert an RDDs to a DataFrames
  val sqlContext = new HiveContext(sc)
  import sqlContext.implicits._

  val customerT = sc.textFile(INPUT_DIR + "/customer.tbl").map(_.split('|')).map(p => Customer(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF()
  val lineitemT = sc.textFile(INPUT_DIR + "/lineitem.tbl").map(_.split('|')).map(p => Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()
  val nationT = sc.textFile(INPUT_DIR + "/nation.tbl").map(_.split('|')).map(p => Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF()
  val regionT = sc.textFile(INPUT_DIR + "/region.tbl").map(_.split('|')).map(p => Region(p(0).trim.toInt, p(1).trim, p(1).trim)).toDF()
  val orderT = sc.textFile(INPUT_DIR + "/orders.tbl").map(_.split('|')).map(p => Order(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)).toDF()
  val partT = sc.textFile(INPUT_DIR + "/part.tbl").map(_.split('|')).map(p => Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()
  val partsuppT = sc.textFile(INPUT_DIR + "/partsupp.tbl").map(_.split('|')).map(p => Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim)).toDF()
  val supplierT = sc.textFile(INPUT_DIR + "/supplier.tbl").map(_.split('|')).map(p => Supplier(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()

  val dfList = Array(customerT,lineitemT,nationT,regionT,orderT,partT,partsuppT,supplierT)

  // orc/parquet table
  def customer: DataFrame = _
  var lineitem: DataFrame = _
  var nation: DataFrame = _
  var order: DataFrame = _
  var part: DataFrame = _
  var partsupp: DataFrame = _
  var region: DataFrame = _
  var supplier: DataFrame = _

  //var dfList2 = Array(customer,lineitem,nation,region,order,part,partsupp,supplier)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(): Unit

  val format = TpchQuery.format
  val compression = TpchQuery.compression

  // the for(df <- dfList) 得到的df is value, 函数内局部变量必须初始化
  // 3.14 Arvin
//  for(i <- 0 until 7){
//    println("initialize df" + compression)
//    dfList2(i) = sc.textFile(OUTPUT_DIR + "/" + compression + "/" + i + ".orc").toDF()
//    if(dfList2(i) == null)
//      println("after initialize still null")
//
//
//  }
  if(format == "orc"){
    customer = sqlContext.read.orc(OUTPUT_DIR + "/" + compression + "/" + 0 + ".orc")
    lineitem = sqlContext.read.orc(OUTPUT_DIR + "/" + compression + "/" + 1 + ".orc")
    nation = sqlContext.read.orc(OUTPUT_DIR + "/" + compression + "/" + 2 + ".orc")
    region = sqlContext.read.orc(OUTPUT_DIR + "/" + compression + "/" + 3 + ".orc")
    order = sqlContext.read.orc(OUTPUT_DIR + "/" + compression + "/" + 4 + ".orc")
    part = sqlContext.read.orc(OUTPUT_DIR + "/" + compression + "/" + 5 + ".orc")
    partsupp = sqlContext.read.orc(OUTPUT_DIR + "/" + compression + "/" + 6 + ".orc")
    supplier = sqlContext.read.orc(OUTPUT_DIR + "/" + compression + "/" + 7 + ".orc")


  }  else if(format == "parquet"){
    customer = sqlContext.read.parquet(OUTPUT_DIR + "/" + compression + "/" + 0 + ".parquet")
    lineitem = sqlContext.read.parquet(OUTPUT_DIR + "/" + compression + "/" + 1 + ".parquet")
    nation = sqlContext.read.parquet(OUTPUT_DIR + "/" + compression + "/" + 2 + ".parquet")
    region = sqlContext.read.parquet(OUTPUT_DIR + "/" + compression + "/" + 3 + ".parquet")
    order = sqlContext.read.parquet(OUTPUT_DIR + "/" + compression + "/" + 4 + ".parquet")
    part = sqlContext.read.parquet(OUTPUT_DIR + "/" + compression + "/" + 5 + ".parquet")
    partsupp = sqlContext.read.parquet(OUTPUT_DIR + "/" + compression + "/" + 6 + ".parquet")
    supplier = sqlContext.read.parquet(OUTPUT_DIR + "/" + compression + "/" + 7 + ".parquet")

  }


//  if(lineitem == null)
//    println("lineitem is null after initial array ")

  // to be called by Q01-Q22 explicitly or called in this parent class,
  // use array of df to initialize df is no effect!!!
  // 3.15 Arvin
  def initializeDf(): Unit ={

  }

  val atomicInt:AtomicInteger = new AtomicInteger()
  // output query, first load table from orc/parquet file; then execute query
  def outputDF(df: DataFrame): Unit = {

    val startTime = System.currentTimeMillis()

    df.write.mode("overwrite").json(OUTPUT_DIR + "/" + atomicInt.getAndIncrement() + ".out")

    val endTime = System.currentTimeMillis()
    println("outputDf " + format + "'s time used is " + (endTime - startTime))

    // should stop sc
    // 3.16 Arvin
    sc.stop()

  }

  def snappy(): Unit ={
    sc.hadoopConfiguration.set("hive.exec.orc.default.compress","SNAPPY")
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
  }


  def uncompressed(): Unit ={
    sc.hadoopConfiguration.set("hive.exec.orc.default.compress","NONE")
    sqlContext.setConf("spark.sql.parquet.compression.codec","uncompressed")
  }

  // write orc or parquet file with different compression
  // different compression use different directories.
  // 3.14 Arvin
  def writeTbl(): Unit ={

    // use snappy or uncompressed or default
    if(compression == "snappy")
      snappy()
    else if(compression == "uncompressed")
      uncompressed()

    val startTime = System.currentTimeMillis()


    if(format == "orc"){
      var i = 0
      while(i < dfList.length)
      {
        dfList(i).write.mode("overwrite").orc(OUTPUT_DIR + "/" + compression + "/" + i + ".orc")
        i += 1
      }
    }
    else if(format == "parquet"){
      var i = 0
      while(i < dfList.length) {
        dfList(i).write.mode("overwrite").parquet(OUTPUT_DIR + "/" + compression + "/" + i + ".parquet")
        i += 1
      }
    }
    val endTime = System.currentTimeMillis()
    println("write all tables with " + format + "'s time used is " + (endTime - startTime))

  }

}

// the query time contains the time to load orc/parquet and write output to hdfs.
object TpchQuery {


  /**
   * Execute query reflectively
   */
  def executeQuery(queryNo: Int): Unit = {
    assert(queryNo >= 1 && queryNo <= 22, "Invalid query number")
    Class.forName(f"Q${queryNo}%02d").newInstance.asInstanceOf[{ def execute }].execute
  }

  var format:String = _
  var compression:String = _

  def main(args: Array[String]): Unit = {

    require(args.length > 2)

    // orc or parquet
    format = args(0)

    // on or off
    compression = args(1)

    // load or query
    val operationType = args(2)

    if(operationType == "load")
      {
        new TpchQuery {
          /**
           * implemented in children classes and hold the actual query
           */
          override def execute(): Unit = ???
        }.writeTbl()
      }
    else if(operationType == "query"){

      val startTime = System.currentTimeMillis()

      // only one sparkContext can be existed in one jvm by default.
      // so execute one query every time and calculate the average.
      // 3.15 Arvin

      executeQuery(args(3).toInt)
//      var num = 1
//      while(num < 23 ){
//        executeQuery(num)
//        num += 1
//      }

      val endTime = System.currentTimeMillis()
      println("query all tables with " + format + "'s time used is " + (endTime - startTime))
    }


  }

}

