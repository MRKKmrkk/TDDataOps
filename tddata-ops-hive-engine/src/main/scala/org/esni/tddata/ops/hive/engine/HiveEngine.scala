package org.esni.tddata.ops.hive.engine

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.esni.tddata.ops.hive.engine.exception.HQLError
import org.esni.tddata.ops.hive.engine.table.HiveTable


class HiveEngine(private val session: SparkSession) {

  private var isEnableDynamicPartition: Boolean = false

  def this(hiveMetastoreUri: String, scratchDir: String, master: String = "") = {

    this{

      val conf: SparkConf = new SparkConf()
        .setAppName("HiveEngine")
        .set("hive.metastore.uris", hiveMetastoreUri)
        .set("hive.exec.scratchdir", scratchDir)
        // todo: 加这个参数才能跑，我也不知道怎么办
        .set("dfs.client.use.datanode.hostname", "true")

      if (master != null && master.nonEmpty) conf.setMaster(master)

      SparkSession
        .builder()
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()

    }

  }

  /**
   * 启动动态分区
   */
  def enableDynamicPartition(): Unit = {

    if (isEnableDynamicPartition) return

    session.sql("set hive.exec.dynamic.partition = true")
    session.sql("set hive.exec.dynamic.partition.mode = nonstrict")

    isEnableDynamicPartition = true

  }

  def createHiveTable(table: HiveTable, saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {

    val writer = session
      .createDataFrame(session.sparkContext.emptyRDD[Row], table.schema)
      .write
      .mode(saveMode)
      .option("fileFormat", table.format.value)

    // 如果是外部表则指定文件位置
    if (table.isExternalTable) {
      writer.option("path", table.path)
    }

    // 如果是分区表则启动分区并设置分区字段
    if (table.isPartitionTable) {
      if (!isEnableDynamicPartition) enableDynamicPartition()
      writer.partitionBy(table.partitionColumns: _*)
    }

    writer.saveAsTable(f"${table.workspace}.${table.layerName}_${table.modelName}")

  }

  // todo: 待实现权限检查
  def executeHQLOnWorkspace(hql: String): DataFrame = {

    if (hql == null || hql.isEmpty) return session.emptyDataFrame

    try {
      session.sql(hql)
    } catch {
      case e: Exception => throw HQLError(e.toString)
    }

  }

  def close(): Unit = session.close()

}


object HiveEngine {

  def apply(hiveMetastoreUri: String, scratchDir: String, master: String): HiveEngine = new HiveEngine(hiveMetastoreUri, scratchDir, master)

}
