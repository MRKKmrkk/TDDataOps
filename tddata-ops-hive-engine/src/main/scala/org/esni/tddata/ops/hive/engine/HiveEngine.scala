package org.esni.tddata.ops.hive.engine

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.esni.tddata.ops.hive.engine.model.Model

import java.util.Properties

class HiveEngine(private val session: SparkSession) {

  private var mysqlUri: String = _
  private var mysqlUser: String = _
  private var mysqlPassword: String = _

  def this(hiveMetastoreUri: String, scratchDir: String, master: String, mysqlUri: String, mysqlUser: String, mysqlPassword: String) = {

    this{

      val conf: SparkConf = new SparkConf()
        .setMaster(master)
        .setAppName("HiveEngine")
        .set("hive.metastore.uris", hiveMetastoreUri)
        .set("hive.exec.scratchdir", scratchDir)

      SparkSession
        .builder()
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()

    }

    this.mysqlUri = mysqlUri
    this.mysqlUser = mysqlUser
    this.mysqlPassword = mysqlPassword

    // 启动动态分区
    // todo: 待测试
    session.sql("set hive.exec.dynamic.partition = true")
    session.sql("set hive.exec.dynamic.partition.mode = nonstrict")

  }

  private def getDataFrameFromMysql(tableName: String): DataFrame = {

    val properties = new Properties()
    properties.put("user", mysqlUser)
    properties.put("password", mysqlPassword)

    session
      .read
      .jdbc(mysqlUri, tableName, properties)

  }

  def createModel(model: Model): Unit = {


  }

}

object HiveEngine {

  def apply(hiveMetastoreUri: String, scratchDir: String, master: String, mysqlUri: String, mysqlUser: String, mysqlPassword: String): HiveEngine = new HiveEngine(hiveMetastoreUri, scratchDir, master, mysqlUri, mysqlUser, mysqlPassword)


}
