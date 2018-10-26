import org.apache.spark.sql.SparkSession


/**
  * Trait to make use of loan pattern to get spark sessions
  */
trait SparkSessionSetup {
  def withSparkSession(f: SparkSession => Any) : Unit = {
    val spark = SparkSession
      .builder()
      .appName("CDRAggregator")
      .master("local[*]")
      .getOrCreate()
    try {
      f(spark)
    }
    finally spark.stop()
  }
}
