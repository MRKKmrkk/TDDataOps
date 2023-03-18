package org.esni.tddata.ops.hive.engine.task

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class HiveTaskActuator private (private val session: SparkSession, private val hql: String)
  extends AutoCloseable{

  def this(hiveMetastoreUri: String, scratchDir: String, hql: String) = {

    this(
      {
        val conf: SparkConf = new SparkConf()
          .setAppName(f"HiveTaskActuator_${System.currentTimeMillis()}")
          .set("hive.metastore.uris", hiveMetastoreUri)
          .set("hive.exec.scratchdir", scratchDir)
          // todo: 加这个参数才能跑，我也不知道怎么办
          .set("dfs.client.use.datanode.hostname", "true")

        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      },
      hql
    )

  }

  def execute(): Unit = {

    session.sql(hql)

  }

  override def close(): Unit = session.close()

}

object HiveTaskActuator {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) throw new IllegalArgumentException("HiveTaskActuator need 3 arguments")
    if (args(0).isEmpty) throw new IllegalArgumentException("argument 'hiveMetastoreUri' for HiveTaskActuator can not be empty")
    if (args(1).isEmpty) throw new IllegalArgumentException("argument 'scratchDir' for HiveTaskActuator can not be empty")
    if (args(2).isEmpty) throw new IllegalArgumentException("argument 'hql' for HiveTaskActuator can not be empty")

    val actuator = new HiveTaskActuator(args(0), args(1), args(2))
    actuator.execute()
    actuator.close()

  }

}
