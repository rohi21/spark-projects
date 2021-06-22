import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object CreateTable {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    System.setProperty("hadoop.home.dir","c:\\winutils")

    val spark = SparkSession.builder().appName("Spark Hive APP").master("local[*]").enableHiveSupport().getOrCreate()

    val  friendsDf = spark.read.option("header","true").option("inferschema","true").csv("C:/SparkScalaCourse/SparkHivePractise/data/fakefriends.csv")

    friendsDf.write.mode(SaveMode.Overwrite).saveAsTable("friendsData3")

    import spark.sql
    sql("SELECT * FROM friendsData3").show()
    sql("SELECT count(*) FROM friendsData3").show()
    sql("DROP TABLE friendsData").show()


    spark.stop()

  }

  }
