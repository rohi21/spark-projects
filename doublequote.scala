import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object doublequote {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("double quotes").master("local[*]").enableHiveSupport().getOrCreate()

    val ipfile =  spark.read
      .option("header","true")
      .option("inferschema", "true")
      //.option("quote", "\"")
    //  .option("escape", "\"")
      .option("delimiter",",")
      .textFile("data/doublequotefile.csv")
    import spark.implicits._
    val newdf= ipfile.flatMap(x => x.split(","))
    newdf.show(false)
    spark.stop()

  }

}
