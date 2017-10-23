import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class ChallengeAppTests extends FunSuite {

  test("[TEST] Creating SparkSession") {
    var sparkSession = SparkSession.builder()
      .appName("ChallengeApp")
      .master("local[2]")
      .config("some-config","test")
      .getOrCreate()

    assert(sparkSession.conf.get("some-config") == "test")
  }

  test("[TEST] Number of records (genres) is 40") {
    var sparkSession = SparkSession.builder().getOrCreate()

    val df1 = sparkSession.sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header",true)
      .csv("hdfs://localhost:54310/maf-datalake/anime.csv")
      .withColumn("genre", split(col("genre"), ", "))

    val df2 = df1.selectExpr("anime_id","name","genre","type",
      "cast(episodes as int) episodes",
      "cast(rating as double) rating",
      "cast(members as int) members")

    df2.createOrReplaceTempView("anime") // Register the DataFrame as a SQL temporary view
    val tvSeries = sparkSession.sqlContext.sql("SELECT * FROM anime WHERE type = 'TV'")

    val res = tvSeries.where(col("episodes") > 10)
      .select(explode(col("genre")) as "g", col("rating"))
      .groupBy("g")
      .agg(sum("rating").alias("r"))
      .sort(col("r").desc)

    assert(res.count() == 40)
  }

  test("[TEST] Ten most rated genres are Comedy") {
    var sparkSession = SparkSession.builder().getOrCreate()

    val df1 = sparkSession.sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header",true)
      .csv("hdfs://localhost:54310/maf-datalake/anime.csv")
      .withColumn("genre", split(col("genre"), ", "))

    val df2 = df1.selectExpr("anime_id","name","genre","type",
      "cast(episodes as int) episodes",
      "cast(rating as double) rating",
      "cast(members as int) members")

    df2.createOrReplaceTempView("anime") // Register the DataFrame as a SQL temporary view
    val tvSeries = sparkSession.sqlContext.sql("SELECT * FROM anime WHERE type = 'TV'")

    val res = tvSeries.where(col("episodes") > 10)
      .select(explode(col("genre")) as "g", col("rating"))
      .groupBy("g")
      .agg(sum("rating").alias("r"))
      .sort(col("r").desc)

    assert(res.count() == 40)
  }
}
