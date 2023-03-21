package org.esni.tddata.ops.hive.engine

import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

object HiveExecuteEngine {

  def main(args: Array[String]): Unit = {

    val hiveMetastoreUri ="thrift://Esni-Master:9083"
    val scratchDir ="hdfs://Esni-Master:8020/tmp/hive"

    val launcher = new SparkLauncher()
      .setAppResource("hdfs://1.15.135.178:8020/tddata_ops/jars/tddata-ops-hive-engine-0.0.1-jar-with-dependencies.jar")
      .setMainClass("org.esni.tddata.ops.hive.engine.task.HiveTaskActuator")
      .setMaster("yarn")
      .addAppArgs(hiveMetastoreUri, scratchDir, "show databases")
      .startApplication(new SparkAppHandle.Listener {
        override def stateChanged(handle: SparkAppHandle): Unit = {
          println(handle.getState.toString)
        }

        override def infoChanged(handle: SparkAppHandle): Unit = {

        }
      })

//    while (true) {
//      println(launcher.getState.toString)
//    }

  }

}
