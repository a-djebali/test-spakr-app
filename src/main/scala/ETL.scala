import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, split}

object ETL {
  def CastDataTypes (DataSource: DataFrame): DataFrame = {

    // Transform genre column to an array of string values
    val df1 = DataSource.withColumn("genre", split(col("genre"), ", "))

    // Cast DataTypes (episodes to int, rating to double, members to int)
    val df2 = df1.selectExpr("anime_id","name","genre","type",
      "cast(episodes as int) episodes",
      "cast(rating as double) rating",
      "cast(members as int) members")

    df2
  }
}