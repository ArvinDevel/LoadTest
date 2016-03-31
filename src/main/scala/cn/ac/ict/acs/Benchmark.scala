/**
 * Created by arvin on 16-3-21.
 */

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.sys.process._


/**
 * Utility class to benchmark components. An example of how to use this is:
 *  val benchmark = new Benchmark("My Benchmark", valuesPerIteration)
 *   benchmark.addCase("V1")(<function>)
 *   benchmark.addCase("V2")(<function>)
 *   benchmark.run
 * This will output the average time to run each function and the rate of each function.
 *
 * The benchmark function takes one argument that is the iteration that's being run.
 *
 * If outputPerIteration is true, the timing for each run will be printed to stdout.
 */
class Benchmark(
                 name: String,
                 valuesPerIteration: Long,
                 iters: Int = 5,
                 outputPerIteration: Boolean = false) {
  val benchmarks = mutable.ArrayBuffer.empty[Benchmark.Case]

  def addCase(name: String)(f: Int => Unit): Unit = {
    benchmarks += Benchmark.Case(name, f)
  }

  /**
   * Runs the benchmark and outputs the results to stdout. This should be copied and added as
   * a comment with the benchmark. Although the results vary from machine to machine, it should
   * provide some baseline.
   */
  def run(): Unit = {
    require(benchmarks.nonEmpty)
    // scalastyle:off
    println("Running benchmark: " + name)

    val results = benchmarks.map { c =>
      println("  Running case: " + c.name)
      Benchmark.measure(valuesPerIteration, iters, outputPerIteration)(c.fn)
    }
    println

    val firstBest = results.head.bestMs

    printf("%-35s %16s %12s %13s %10s\n", name + ":", "Best/Avg Time(ms)", "Rate(M/s)",
      "Per Row(ns)", "Relative")
    println("-----------------------------------------------------------------------------------" +
      "--------")
    results.zip(benchmarks).foreach { case (result, benchmark) =>
      printf("%-35s %16s %12s %13s %10s\n",
        benchmark.name,
        "%5.0f / %4.0f" format (result.bestMs, result.avgMs),
        "%10.1f" format result.bestRate,
        "%6.1f" format (1000 / result.bestRate),
        "%3.1fX" format (firstBest / result.bestMs))
    }
    println
    // scalastyle:on
  }
}

object Benchmark {
  case class Case(name: String, fn: Int => Unit)
  case class Result(avgMs: Double, bestRate: Double, bestMs: Double)


  /**
   * clr worker's memory by call shell script.
   *
   */
  def clrCache(): Unit = {
    if (new java.io.File("/home/pandatest/clrCache.sh").exists) {
      val commands = Seq("bash", "-c", s"/home/netflow/clrCache.sh")
      commands.!!
      System.err.println("free_memory succeed")
    } else {
      System.err.println("free_memory script doesn't exists")
    }
  }

  /**
   * Runs a single function `f` for iters, returning the average time the function took and
   * the rate of the function.
   * before iter, clr worker's cache
   */
  def measure(num: Long, iters: Int, outputPerIteration: Boolean)(f: Int => Unit): Result = {
    val runTimes = ArrayBuffer[Long]()
    for (i <- 0 until iters + 1) {

      clrCache()

      val start = System.nanoTime()

      f(i)

      val end = System.nanoTime()
      val runTime = end - start
      if (i > 0) {
        runTimes += runTime
      }

      if (outputPerIteration) {
        // scalastyle:off
        println(s"Iteration $i took ${runTime / 1000} microseconds")
        // scalastyle:on
      }
    }
    val best = runTimes.min
    val avg = runTimes.sum / iters
    Result(avg / 1000000.0, num / (best / 1000.0), best / 1000000.0)
  }

  // use Benchmark to test tpch query
  // 3.21 Arvin
  //ToDo know the valuesPerinter's meaning(benchmark's second parameter)
   def main (args: Array[String]){
     val benchmark = new Benchmark("My Benchmark", 1000000,4,true)
     benchmark.addCase("V1")(encapQuery(1))
     benchmark.addCase("V2")(encapQuery(2))
     benchmark.run
  }

  // encapsulate the 22 queries
  def encapQuery(index: Int)(iter: Int): Unit = {

     TpchQuery.executeQuery(index)

  }


}


