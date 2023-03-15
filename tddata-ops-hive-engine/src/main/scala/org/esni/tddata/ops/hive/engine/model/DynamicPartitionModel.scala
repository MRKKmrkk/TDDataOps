package org.esni.tddata.ops.hive.engine.model

import org.apache.spark.sql.types.StructType

class DynamicPartitionModel(
                             var partitionCols: Array[String],
                             workspace: String,
                             layerName: String,
                             modelName: String,
                             schema: StructType,
                             storagePath: String,
                             format: ModelFormat,
                             bucketCols: Array[String],
                             bucketNumber: Int
                           ) extends HiveModel(workspace, layerName, modelName, schema, storagePath, format, bucketCols, bucketNumber) {

}
