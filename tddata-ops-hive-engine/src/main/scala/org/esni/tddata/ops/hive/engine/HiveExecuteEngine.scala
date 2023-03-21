package org.esni.tddata.ops.hive.engine

import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import org.slf4j.LoggerFactory

import java.util.concurrent.CountDownLatch

object HiveExecuteEngine {

  def main(args: Array[String]): Unit = {

    val hiveMetastoreUri ="thrift://Esni-Master:9083"
    val scratchDir ="hdfs://Esni-Master:8020/tmp/hive"
    val logger = LoggerFactory.getLogger(HiveExecuteEngine.getClass)

    val cd = new CountDownLatch(1)

    new SparkLauncher()
      .setAppResource("hdfs://1.15.135.178:8020/tddata_ops/jars/tddata-ops-hive-engine-0.0.1-jar-with-dependencies.jar")
      .setMainClass("org.esni.tddata.ops.hive.engine.task.HiveTaskActuator")
      .setMaster("yarn")
      .addAppArgs(hiveMetastoreUri, scratchDir, "create database cd_01")
      .setDeployMode("client")
      .startApplication(new SparkAppHandle.Listener {
        override def stateChanged(handle: SparkAppHandle): Unit = {
          logger.info("============================" + handle.getState.toString + "==============================")
        }

        override def infoChanged(handle: SparkAppHandle): Unit = {

        }
      })
    cd.await()

//    while (true) {
//      println(launcher.getState.toString)
//    }

  }

}
