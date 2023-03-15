package org.esni.tddata.ops.hive.engine

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.esni.tddata.ops.hive.engine.model.{HiveModel, ModelFormat}

object Demo {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    val hiveMetastoreUri ="thrift://ESNI-Master:9083"
    val scratchDir ="hdfs://ESNI-Master:8020/tmp/hive"
    val master ="local[*]"
    val engine = new HiveEngine(hiveMetastoreUri,scratchDir,master)

    val structType = StructType(
      Array(
      StructField("name", StringType, true),
      StructField("sex", StringType, true)
      )
    )

    val model = HiveModel("test2", "ods", "testmodel", structType, "hdfs://ESNI-Master:8020/user/hive/warehouse/test", ModelFormat.PARQUET, Array("name"), 3)


    engine.createModel(model)

    engine.close()



  }
}
