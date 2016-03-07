import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by arvin on 16-2-19.
 *
 * original-file has the same size,存储后的大小不一样， 这也不能说明存储时是处理的字段类型不一样，
 * 因为内容不一样，即使大小一样，压缩存储后的文件大小也不一样。
 * 另外按一定数目的行数进行测试，原始文件大小差异太大，我感觉文件大小的因素的分量更重。
 *
 * 因此刚开始我是每种类型都写入10^8行，后来是写入固定大小的文件；
 * 另一方面，数字类型的我一直当做string来处理的，没有引入case class,后来进行了纠正。
 *
 */



case class IntClass(name: Int)
case class LongClass(name: Long)
case class DoubleClass(name: Double)
case class StringClass(name: String)

object WritePerformance{
  def main(args: Array[String]): Unit ={
    if(args.length < 2)
      println("args is not enough!")
    // field type
    // 2.23 Arvin
    // int, long, double, string
    val fieldType = args(0)
    // orc or parquet
    val flag = args(1)
    // file path
    val path = args(2)


    // 凑，　我怎么还留有setMaster("local")! debug several hours
    // 2.22 Arvin
//    val conf = new SparkConf().setMaster("local").setAppName("PerformanceTest")
    val conf = new SparkConf().setAppName("WritePerformance")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("hive.exec.orc.default.compress","SNAPPY")


    //set orc and parquet config
//    sc.hadoopConfiguration.set("parquet.blocksize","10")

    val sqlContext = new HiveContext(sc)
    // tell spark read file from local filesystem, or it will read from hdfs.(default)
    //2.22 Arvin
    // but it seems that read from local has problem
    val rdd = sc.textFile("/user/pandatest/" + path)
    import sqlContext.implicits._

    // use snappy
    // 2.23 Arvin
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")



    // use case class to make sure that the field type in table is guaranteed.
    // 2.23 Arvin
    // use df.count() or collect to execute the converting from rdd.

    var df: DataFrame = null
    if(fieldType.equals("int")){
      df = rdd.map(name => IntClass(name.trim.toInt))toDF()
//      df.collect()

    }
    else if(fieldType.equals("long")){
      df = rdd.map(name => LongClass(name.trim.toLong))toDF()
//      df.collect()

    }
    else if(fieldType.equals("double")){
      df = rdd.map(name => DoubleClass(name.trim.toDouble))toDF()
//      df.collect()

    }else if(fieldType.equals("string")){
      df = rdd.map(name => StringClass(name))toDF()
//      df.collect()
    }
    else{
      println("the fieldType is wrong!")
      return
    }



    val startTime = System.currentTimeMillis()

    if(flag.equals("orc"))
      df.write.orc ( path + ".orc")
    else if(flag.equals("parquet"))
      df.write.parquet( path + ".parquet")
    else
      println("write file format is wrong! (use orc or parquet)!")

    val endTime = System.currentTimeMillis()
    println("write " + flag + "'s time used is " + (endTime - startTime))

    sc.stop()
  }

}
