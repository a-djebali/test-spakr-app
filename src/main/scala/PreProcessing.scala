import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types._

object PreProcessing {
  def CastDataTypes (DataSource: DataFrame): DataFrame = {

    // Transform genre column to an array of string values
    // Cast DataTypes (episodes to int, rating to double, members to int)

    DataSource.withColumn("genre", split(col("genre"), ", "))
      .select(
        col("anime_id").cast(IntegerType),
        col("name"),
        col("genre"),
        col("type"),
        col("episodes").cast(IntegerType),
        col("rating").cast(DoubleType),
        col("members").cast(IntegerType)
      )
  }
}