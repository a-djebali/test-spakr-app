import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{col, explode, sum}

object Recommendations {
  def TopTenMostRatedTvSeries (sparkSession: SparkSession, DataSource: DataFrame): DataFrame = {

    // Select only TV series
    DataSource.createOrReplaceTempView("anime") // Register the DataFrame as a SQL temporary view
    val df1 = sparkSession.sqlContext.sql("SELECT * FROM anime WHERE type = 'TV'")

    // Run the query that determines the top 10 most rated TV series' genres with over 10 episodes.
    val df2 = df1.where(col("episodes") > 10)
      .select(explode(col("genre")) as "g", col("rating"))
      .groupBy("g")
      .agg(sum("rating").alias("r"))
      .sort(col("r").desc)

    df2
  }

  // Other applications

  def UserRecommendedGenres (): Unit = {
    // ....
  }


  def UserRecommendedTvSeries (): Unit = {
    // ....
  }

  def UserRecommendedTvMovies (): Unit = {
    // ....
  }
}