import org.apache.log4j.{Level, Logger}
import org.scalatest.{BeforeAndAfterAll, FunSpec, FunSuite}

class CDRAggregatorSpec extends FunSuite with SparkSessionSetup with BeforeAndAfterAll{
  override protected def beforeAll(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
  }

  test("CDRAggregator") {
    withSparkSession(spark => {
      import spark.implicits._
        val cdrPath = "src/main/resources/cdrs.csv"
        val ds = CDRAggregator.loadData[Cdr](spark, cdrPath)
        ds.show
        assert(ds.count() == 9)

    })
    assert(true)
  }

  //TODO should be more to test aggregate function

}
