package org.esni.tddata.ops.hive.engine.model
import org.apache.spark.sql.types.StructType

class HiveModel(
                var workspace: String,
                var layerName: String,
                var modelName: String,
                var schema: StructType,
                var storagePath: String,
                var format: ModelFormat,
                var bucketCols: Array[String],
                var bucketNumber: Int
               ) extends Model {


  def isBucketTable: Boolean = bucketCols.nonEmpty && bucketNumber > 0

}

object HiveModel {

  // 创建分桶表
  def apply(workspace: String, layerName: String, modelName: String, schema: StructType, storagePath: String, format: ModelFormat, bucketCols: Array[String], bucketNumber: Int): HiveModel = new HiveModel(workspace, layerName, modelName, schema, storagePath, format, bucketCols, bucketNumber)

  // 创建非分桶表
  def apply(workspace: String, layerName: String, modelName: String, schema: StructType, storagePath: String, format: ModelFormat): HiveModel = new HiveModel(workspace, layerName, modelName, schema, storagePath, format, Array(), -1)

}
