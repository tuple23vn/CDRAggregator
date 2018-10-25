import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CDRAggregator {
  def main(args: Array[String]) : Unit =  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("init main")
  }

}
