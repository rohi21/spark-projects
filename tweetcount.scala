

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions.{col, lit, when}


object tweetcount {

  case class tweetdata(id: Int,
                       user_id: Int,
                       text: String,
                       timestamp: String)
  case class userdt(id: Int, username: String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("tweet count").master("local[*]").getOrCreate()
   import spark.implicits._
    val tweetrdd =  spark.read
      .option("header","true")
      .option("inferschema", "true")
      .csv("data/Tweet.csv")
      .as[tweetdata]
    val userdd =  spark.read
      .option("header","true")
      .option("inferschema", "true")
      .csv("data/User.csv")
      .as[userdt]

    val totaltweets = tweetrdd.groupBy("user_id").count()

    val finaldf1 = totaltweets.as("twe")
      .join(userdd.as("usr"),
        col("twe.user_id") === col("usr.id"), "outer"
          )
      . withColumn("count", when(col("count").isNull, 0).otherwise(col("count"))).select("username","count","id")
    //println("username,count")
    //finaldf1.show(finaldf1.count.toInt)
    val finaldf = finaldf1.orderBy(col("count").desc,col("id").asc)
    finaldf.show(finaldf.count.toInt)
    val results = finaldf.collect()
    for (result <- results) {

     println(result(0)+ "," +result(1))
   }

  }
}
