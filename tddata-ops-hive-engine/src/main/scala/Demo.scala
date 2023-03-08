import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Demo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("connect hive")
      .setMaster("local[*]")
      .set("hive.exec.scratchdir", "hdfs://ESNI-Master:8020/tmp/hive")


    val session = SparkSession
      .builder()
      .config(conf)
      .config("hive.metastore.uris", "thrift://ESNI-Master:9083")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    session
      .sql(
        """
          |show databases
          |""".stripMargin)
      .show()

  }

}
