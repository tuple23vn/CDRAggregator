import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object AggregatorHelper {

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
    * Given 2 datasets of CDR and Cell, return the total dataset
    * @param spark spark context of the run
    * @param cdrDS dataset represents the CDR
    * @param cellDS dataset represents the Cell info
    * @return the final dataset
    */
  def aggregate(spark: SparkSession, cdrDS: Dataset[Cdr], cellDS: Dataset[Cell]) : Dataset[AggregatedCdr] = {
    import spark.implicits._


    /**
      * Calculate simple metrics that can be achieved with built in functions of sparksql
      */
    val simpleMetricsDF = cdrDS
      .withColumn("international_duration",
        when('type === "international", col("duration")).otherwise(lit(0)))
      .withColumn("on_net_duration",
        when('type === "on-net", col("duration")).otherwise(lit(0)))
      .withColumn("less_10min",
        when('duration <= 10.0, lit(1)).otherwise(lit(0))).groupBy("caller_id")
      .agg(
        countDistinct("callee_id") as "total_distinct_callees",
        sum('dropped) as "total_dropped",
        sum('duration) as "total_duration",
        sum('international_duration) as "total_international_duration",
        avg('on_net_duration) as "avg_on_net_duration",
        sum('less_10min) as "less_than_10min")


    /**
      * Calculate complex metrics using UDAF following Aggregator pattern
      */
    val complexMetricAggregator = new Aggregator[Row, BufferAccumulator, OutputAccumulator] {
      override def zero: BufferAccumulator = BufferAccumulator(Map.empty, Map.empty)

      override def reduce(b: BufferAccumulator, a: Row): BufferAccumulator = {
        val newCellId: String = a.getAs("cell_id")
        val newCellInfo = b.cellInfo.updated(newCellId, b.cellInfo.getOrElse(newCellId, 0) + 1)
        val newCallee: String = a.getAs("callee_id")
        val newCalleeInfo = b.callees.updated(newCallee, b.callees.getOrElse(newCallee, 0) + 1)
        BufferAccumulator(newCellInfo, newCalleeInfo)
      }

      override def merge(b1: BufferAccumulator, b2: BufferAccumulator): BufferAccumulator = {
        val cellInfo = (b1.cellInfo.keys ++ b2.cellInfo.keys).map(k => k -> (b1.cellInfo.getOrElse(k, 0) + b2.cellInfo.getOrElse(k, 0))).toMap
        val calleeInfo = (b1.callees.keys ++ b2.callees.keys).map(k => k -> (b1.callees.getOrElse(k, 0) + b2.callees.getOrElse(k, 0))).toMap
        BufferAccumulator(cellInfo, calleeInfo)
      }

      override def finish(reduction: BufferAccumulator): OutputAccumulator = {
        val topCell = reduction.cellInfo.toSeq.sortWith(_._2 > _._2).head
        val topCallees = reduction.callees.toSeq.sortWith(_._2 > _._2).take(3).map(_._1).toList
        OutputAccumulator(topCell._1, topCell._2, topCallees)
      }

      override def bufferEncoder: Encoder[BufferAccumulator] = implicitly[Encoder[BufferAccumulator]]

      override def outputEncoder: Encoder[OutputAccumulator] = implicitly[Encoder[OutputAccumulator]]
    }

    val cellCrdsDF = cdrDS.groupBy("caller_id")
      .agg(complexMetricAggregator.toColumn.name("result"))
      .withColumn("cell_id", 'result.getItem("favouriteCell"))
      .withColumn("top_cell_count", 'result.getItem("favouriteCellUseCount"))
      .withColumn("top_callee_1", 'result.getItem("favoriteCallees").getItem(0))
      .withColumn("top_callee_2", 'result.getItem("favoriteCallees").getItem(1))
      .withColumn("top_callee_3", 'result.getItem("favoriteCallees").getItem(2))
      .drop('result)

    val mostUsedCellDF = cellCrdsDF.join(cellDS, Seq("cell_id"), "left_outer")
      .withColumnRenamed("cell_id", "top_cell")
      .withColumnRenamed("longitude", "top_cell_longitude")
      .withColumnRenamed("latitude", "top_cell_latitude")

    val aggregatedDF = simpleMetricsDF.join(mostUsedCellDF, Seq("caller_id"), "left_outer")
    aggregatedDF.as[AggregatedCdr]
  }

}

case class Cell(cell_Id: String, longitude: Double, latitude: Double)
case class Cdr(caller_id: String, callee_id: String, cell_id: String, duration: Double, `type`: String, dropped: Int )
case class BufferAccumulator(cellInfo: Map[String, Int], callees: Map[String, Int])
case class OutputAccumulator(favouriteCell: String, favouriteCellUseCount: Int, favoriteCallees: List[String])
case class AggregatedCdr(caller_id: String, total_distinct_callees: String, total_dropped: BigInt, total_duration: Double,
                         total_international_duration: Double, avg_on_net_duration: Double, less_than_10min: BigInt,
                         top_cell: String, top_cell_count: BigInt,
                         top_callee_1: String, top_callee_2: String, top_callee_3: String,
                         top_cell_longitude: Option[Double], top_cell_latitude: Option[Double])
