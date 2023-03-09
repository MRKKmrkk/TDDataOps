package org.esni.tddata.ops.hive.engine.model

import org.apache.spark.sql.types.StructType

class StaticPartitionModel(
                            var partitionMap: Map[String, String],
                            workspace: String,
                            layerName: String,
                            modelName: String,
                            schema: StructType,
                            storagePath: String,
                            format: ModelFormat,
                            bucketCols: Array[String]
                          ) extends Model(workspace, layerName, modelName, schema, storagePath, format, bucketCols) {

}
