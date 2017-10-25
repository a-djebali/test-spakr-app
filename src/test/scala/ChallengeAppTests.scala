import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/**
  * ChallengeAppTests.scala:
    Here lives all the necessary tests for our spark application

  To run from terminal:
      - cd into the ChallengeApp directory
      - sbt test > test-result.txt(test and save the result into a text file)
  */

class ChallengeAppTests extends FunSuite {

  test("[TEST 1] Creating SparkSession") {
    var sparkSession = SparkSession.builder()
      .appName("ChallengeApp")
      .master("local[2]")
      .config("some-config","test")
      .getOrCreate()

    // Use global SparkSession for testing
    assert(sparkSession.conf.get("some-config") == "test")
  }

  test("[TEST 2] Test the query, the number of records must be 40") {
    var sparkSession = SparkSession.builder().getOrCreate()

    val df1 = sparkSession.sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header",true)
      .csv("hdfs://localhost:54310/maf-datalake/anime.csv")

    val df2 = PreProcessing.CastDataTypes(df1)

    val result = Recommendations.TopTenMostRatedTvSeries(sparkSession,df2)

    assert(result.count() == 40)
  }

  /*
  Other tests
    - Test that the result should contain all of the ten most rated genres
    - Test dataframe of the query that determines the ten most rated genres is equal with the expected result
    - Test the values of the query with the values of the expected result (value by value)
    - Test unequal dataframes should not be equal when length differs
    - ...
   */
}