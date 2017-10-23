import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * ChallengeApp.scala:
    This application determines the top 10 most rated TV series' genres with over 10 episodes.
  */

object ChallengeApp {
  def main(args: Array[String]) {

    // Create a SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("ChallengeApp")
      .getOrCreate()

    // Load from HDFS as a DataFrame (while loading the data, genre column is converted to an array of string values)
    val df1 = sparkSession.sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header",true) // We can add ".option("inferSchema",true)" to automatically detect DataTypes, but unfortunately sometimes doesn't work correctly
      .csv("hdfs://localhost:54310/maf-datalake/anime.csv")
      .withColumn("genre", split(col("genre"), ", "))

    // Cast DataTypes (episodes to int, rating to double, members to int)
    val df2 = df1.selectExpr("anime_id","name","genre","type",
      "cast(episodes as int) episodes",
      "cast(rating as double) rating",
      "cast(members as int) members")

    // Select only TV series
    df2.createOrReplaceTempView("anime") // Register the DataFrame as a SQL temporary view
    val df3 = sparkSession.sqlContext.sql("SELECT * FROM anime WHERE type = 'TV'")
    
    // ==> Test 2 - Count == 40
    // ==> Test 3 - Best rated movies  

    // Run the query that determines the top 10 most rated TV series' genres with over 10 episodes.
    val res = df3.where(col("episodes") > 10)
      .select(explode(col("genre")) as "g", col("rating"))
      .groupBy("g")
      .agg(sum("rating").alias("r"))
      .sort(col("r").desc)

    // Print out the result
    println(res.show(10))

    // Store the result in HDFS (to serve frond-end apps via APIs for example)
    // df.write.format("csv").save("/tmp/df.csv")
    // https://community.hortonworks.com/questions/42838/storage-dataframe-as-textfile-in-hdfs.html

    // Show or print the result
    // res.show(10)
    // Or a loop to show ten
    // res.collect.foreach(println)
    // Or

    // Stop the Spark Session
    sparkSession.stop()

  }
}