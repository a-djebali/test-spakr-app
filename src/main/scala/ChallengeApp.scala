import org.apache.spark.sql.SparkSession

/**
  * ChallengeApp.scala:
    This application determines the top 10 most rated TV series' genres with over 10 episodes.

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

    // Extract data from HDFS as a DataFrame
    val df1 = sparkSession.sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header",true) // We can add ".option("inferSchema",true)" to automatically detect DataTypes, but unfortunately sometimes doesn't work correctly
      .csv("hdfs://localhost:54310/maf-datalake/anime.csv")

    // Transform data in a good shape for better querying
    val df2 = ETL.CastDataTypes(df1)

    // Run the query that determines the top 10 most rated TV series' genres with over 10 episodes.
    val result = Queries.TopTenMostRatedTvSeries(sparkSession,df2)

    // Print out the result
    println(result.show(10)) // top 10 most rated TV series' genres with over 10 episodes.
    println(result.count()) // number of records

    // Store the result in HDFS (or as structured data in RDBMS or JSON formt to serve frond-end APPs via APIs for example)
    result.write.format("csv").save("hdfs://localhost:54310/maf-datalake/result.csv")

    // Stop the Spark Session
    sparkSession.stop()
  }
}