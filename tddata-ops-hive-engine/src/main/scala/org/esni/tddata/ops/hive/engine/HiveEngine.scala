package org.esni.tddata.ops.hive.engine

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrameWriter, Row, SaveMode, SparkSession}
import org.esni.tddata.ops.hive.engine.model.{DynamicPartitionModel, HiveModel, Model, StaticPartitionModel}

class HiveEngine(private val session: SparkSession) {

  private var isEnableDynamicPartition: Boolean = false

  def this(hiveMetastoreUri: String, scratchDir: String, master: String) = {

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

  }

  /**
   * 启动动态分区
   */
  def enableDynamicPartition(): Unit = {

    if (isEnableDynamicPartition) return

    // 启动动态分区
    // todo: 待测试
    session.sql("set hive.exec.dynamic.partition = true")
    session.sql("set hive.exec.dynamic.partition.mode = nonstrict")

    isEnableDynamicPartition = true

  }

  private def getDataWriter(model: HiveModel): DataFrameWriter[Row] = {

    val writer = session
      .createDataFrame(session.sparkContext.emptyRDD[Row], model.schema)
      .write
      .mode(SaveMode.ErrorIfExists)
      .format(model.format.value)
      .option("path", model.storagePath)

    // 分桶
    if (model.isBucketTable) {

      if (model.bucketCols.length > 1) {
        writer.bucketBy(model.bucketNumber, model.bucketCols(0))
      } else {
        writer.bucketBy(model.bucketNumber, model.bucketCols(0), model.bucketCols.slice(1, model.bucketCols.length): _*)
      }

    }

    writer

  }

  /**
   * 创建不分区数据模型
   */
  def createModel(model: HiveModel): Unit = getDataWriter(model).saveAsTable(f"${model.layerName}.${model.modelName}")

  /**
   * 创建动态数据分区模型
   */
  def createModelOnDynamicPartition(model: DynamicPartitionModel): Unit = {

    if (!isEnableDynamicPartition) enableDynamicPartition()

    getDataWriter(model)
      .partitionBy(model.partitionCols: _*)
      .saveAsTable(f"${model.layerName}.${model.modelName}")

  }

  /**
   * 创建静态数据分区模型
   */
  def createModelOnStaticPartition(model: StaticPartitionModel): Unit = {

  }


}


object HiveEngine {

  def apply(hiveMetastoreUri: String, scratchDir: String, master: String): HiveEngine = new HiveEngine(hiveMetastoreUri, scratchDir, master)


}
