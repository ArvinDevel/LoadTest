import java.io.{File, PrintWriter}

import scala.io.Source
import scala.util.Random

/**
 * Created by arvin on 16-2-19.
 */

object WriteTestFile{

  def writeIntFile(): Unit ={
    val file = new File("/home/arvin/projects/intFile.txt")

    val pw = new PrintWriter(file)

    var flag = true

    // why does i use row line num?
    // use while can control file size more beautiful.

while(flag){
      if(file.length() > 1342177280)
        flag = false
      // pw.write(10) will treat it as ascii value
      pw.print(Random.nextInt() + "\n")
    }
    pw.close()

  }
  def writeLongFile(): Unit ={
    val file = new File("/home/arvin/projects/longFile.txt")

    val pw = new PrintWriter(file)

    var flag = true

    while(flag){
      if(file.length() > 1342177280)
        flag = false
      pw.print(Random.nextLong() + "\n")
    }
    pw.close()

  }
  def writeIntCsvFile(): Unit ={
    val file = new File("/home/arvin/projects/intCsvFile.txt")

    val pw = new PrintWriter(file)

    for (i <- 0 to 100)
    {

      // pw.write(10) will treat it as ascii value
      pw.print(Random.nextInt() + "," + Random.nextDouble() + "\n")
    }
    pw.close()

  }
  def writeDoubleFile(): Unit ={
    val file = new File("/home/arvin/projects/doubleFile.txt")

    val pw = new PrintWriter(file)


    var flag = true

    while(flag){
      if(file.length() > 1342177280)
        flag = false
      pw.print(Random.nextDouble() + "\n")
    }
    pw.close()

  }
  def writeStringFile(): Unit ={
    val file = new File("/home/arvin/projects/stringFile.txt")

    val pw = new PrintWriter(file)


    var flag = true

    while(flag){
      if(file.length() > 1342177280)
        flag = false
      pw.print(Random.nextString(6) + "\n")
    }
    pw.close()

  }

  def read(): Unit ={
    val source = Source.fromFile("/home/arvin/projects/intFile.txt")

    for(line <- source.getLines())
      {
        val wds = line.split(" ")
        for (i <- 0 until wds.length)
          println(wds(i))

      }

  }
  // write string file which can be read
  // 2.24 Arvin
  def writeReadableString(): Unit ={

    val file = new File("/home/arvin/projects/readableStringFile.txt")

    val pw = new PrintWriter(file)


    var flag = true

    while(flag){
      if(file.length() > 1342177280)
        flag = false

      pw.print(List.fill(8)(Random.nextPrintableChar()).mkString + "\n")
    }


  }
  def main(args: Array[String]): Unit = {
//writeIntCsvFile()
//read()
//    writeIntFile()
//    writeLongFile()
//    writeDoubleFile()
//    writeStringFile
//writeReadableString()
//    testSplit()

    combineNetflow()
  }
  def testSplit(): Unit ={
    val source = Source.fromFile("/home/arvin/projects/netflow5","unicode")
//    val record = ".9.-94.-89.-11 .122.-90.78.-2 .122.-90.78.-2 30720 27648 1280 -6144 318714778112 " +
//      "318714909184 934400 5376 0 1536 0 0 0 0 4864"
//    val p = record.split(' ')
//      val test = Netflow(p(0).trim, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim.toLong,
//      p(5).trim.toLong, p(6).trim.toLong, p(7).trim.toLong, p(8).trim.toLong, p(9).trim.toInt, p(10).trim.toInt, p(11).trim.toInt, p(12).trim.toInt,
//      p(13).trim.toInt, p(14).trim.toLong, p(15).trim.toLong, p(16).trim.toInt, p(17).trim.toInt)

    for(line <- source.getLines()){
      println(line.trim.toLong)
    }

//    val iter = source.getLines().map(_.split(' ')).map(p => Netflow(p(0).trim, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim.toLong,
//      p(5).trim.toLong, p(6).trim.toLong, p(7).trim.toLong, p(8).trim.toLong, p(9).trim.toInt, p(10).trim.toInt, p(11).trim.toInt, p(12).trim.toInt,
//      p(13).trim.toInt, p(14).trim.toLong, p(15).trim.toLong, p(16).trim.toInt, p(17).trim.toInt))
//    while(iter.hasNext){
//      val t = iter.next()
//      println(t.nexthop)
//    }

  }

  // 使用以前的较小的netflow文件拼接成大的netflow文件来进行测试，
  // 上次恢复数据时，忘了恢复original-spark1.5.1中的test文件夹，里面有许多测试文件啊，%>_<%
  // 4.21 Arvin
  def combineNetflow(): Unit ={
    val source = Source.fromFile("/home/arvin/netflow5")
    val source2 = Source.fromFile("/home/arvin/netflow5-1")
    val source3 = Source.fromFile("/home/arvin/netflow5-2")

    val outputFile = new File("/home/arvin/netflowData")

    val pw = new PrintWriter(outputFile)



      for (line <- source.getLines(); line2 <- source2.getLines(); line3 <- source3.getLines()) {

        pw.println(line)
        pw.println(line2)
        pw.println(line3)

      }



    source.close()
    source2.close()
    source3.close()

    pw.close()




  }

}
