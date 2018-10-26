

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import java.io.{File, PrintWriter}



object CDRAggregator extends SparkSessionSetup {
  def main(args: Array[String]) : Unit =  {

    //limit  the logging to ERROR level since spark print a lot
    Logger.getLogger("org").setLevel(Level.ERROR)

    if (args.length < 3) {
      println("Not enough arguments. Argument format: [cdrsPath] [cellsPath] [outputPath]")
      System.exit(1)
    }

    //TODO: should check the validity of the string
    val cdrsPath = args(0)
    val cellsPath = args(1)
    val outputPath = args(2)

    withSparkSession(spark => {
      import spark.implicits._

      val cdrDS = loadData[Cdr](spark, cdrsPath)
      val cellDS = loadData[Cell](spark, cellsPath)
      val aggregatedDF = AggregatorHelper.aggregate(spark, cdrDS, cellDS)
      printResult(aggregatedDF, outputPath)
    })
  }

  /**
    * Load data from csv
    * @param spark spark context of the run
    * @param path the path of the csv file to be parsed
    * @tparam B Type of the dataset
    * @return the dataset
    */
  def loadData[B: Encoder](spark: SparkSession, path: String) : Dataset[B] = {
    spark.read
      .option("header", "true")
      .schema(implicitly[Encoder[B]].schema)
      .csv(path)
      .as[B]
  }

  /**
    * Print the resuilt to output file specified by user
    * @param aggregatedDS the dataset to be print
    * @param outPath the path of the output file
    */
  def printResult(aggregatedDS: Dataset[AggregatedCdr], outPath: String): Unit = {

    def formatOptionalValue(x: Option[_]) : String = {
      x match {
       case Some(d) => d.toString
       case None => "null"
      }
    }

    val pw = new PrintWriter(new File(outPath))

    aggregatedDS.collect().foreach(cdr => {
      pw.println()
      pw.println(s"==== Summary for caller_id ${cdr.caller_id} ====")
      pw.println(s"The most used cell: ${cdr.top_cell}")
      pw.println(s"The number of distinct callees:  ${cdr.total_distinct_callees}")
      pw.println(s"The number of dropped calls:  ${cdr.total_dropped}")
      pw.println(s"The total duration of the calls:  ${cdr.total_duration}")
      pw.println(s"The total duration of the international calls: ${cdr.total_international_duration}")
      pw.println(s"The average duration of the on-net calls: ${cdr.avg_on_net_duration}")
      pw.println(s"The latitude and longitude of the most used cell: (${formatOptionalValue({cdr.top_cell_latitude})}, ${formatOptionalValue({cdr.top_cell_longitude})})")
      pw.println(s"The number of calls that lasted less than 10 minutes (included): ${cdr.less_than_10min}")
      pw.println(s"The number of calls that were relayed by the most used cell: ${cdr.top_cell_count}")
      pw.println(s"The top-3 callee ids: (${cdr.top_callee_1}, ${cdr.top_callee_2}, ${cdr.top_callee_3})")
    })
    pw.close()
  }
}
