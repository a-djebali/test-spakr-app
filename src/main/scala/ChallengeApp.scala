import org.apache.spark.sql.SparkSession

/**
  * ChallengeApp.scala:
    App1 (Recommendation 1): determines the top 10 most rated TV series' genres with over 10 episodes.
    App2 (Recommendation 2): ...

  To run from terminal:
      - cd into the ChallengeApp directory
      - sbt package
      - spark-submit --class "ChallengeApp" --master local[2] target/scala-2.11/challengeapp_2.11-0.1.jar
  */

object ChallengeApp {

  // Create a SparkSession
  val sparkSession = SparkSession
    .builder()
    .appName("ChallengeApp")
    .getOrCreate()

  def main(args: Array[String]) {

    // ==> App1

    // Extract data from HDFS as a DataFrame
    val df1 = sparkSession.sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header",true) // We can add ".option("inferSchema",true)" to automatically detect DataTypes, but unfortunately sometimes doesn't work correctly
      .csv("../../test/resources/data/anime.csv") // or read directly from the file
      // ../src/test/resources/data
    // Transform data in a good shape for better querying
    val df2 = PreProcessing.CastDataTypes(df1)

    // Run the query that determines the top 10 most rated TV series' genres with over 10 episodes.
    val result = Recommendations.TopTenMostRatedTvSeries(sparkSession,df2)

    // Print out the result
    println(result.show(10)) // top 10 most rated TV series' genres with over 10 episodes => Below

    /*

    +-------------+------------------+
    |        Genre|            Rating|
    +-------------+------------------+
    |       Comedy|11763.490000000002|
    |       Action| 7238.850000000001|
    |    Adventure| 6088.099999999998|
    |       Sci-Fi| 5153.979999999994|
    |      Shounen| 4871.370000000004|
    |        Drama|           4854.77|
    |      Fantasy| 4853.599999999995|
    |      Romance|           4466.76|
    |       School| 3758.240000000004|
    |Slice of Life| 3613.220000000001|
    +-------------+------------------+

     */
    println(result.count()) // number of records => 40

    // Store the result in HDFS (or as structured data in RDBMS or JSON formt to serve frond-end APPs via APIs for example)
    result.write.format("csv").save("hdfs://localhost:54310/maf-datalake/result.csv")


    // ==> App2 ...

    // Stop the Spark Session
    sparkSession.stop()
  }
}