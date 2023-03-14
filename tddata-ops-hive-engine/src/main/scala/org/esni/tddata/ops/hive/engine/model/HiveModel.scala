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


  def isBucketTable: Boolean = bucketCols.nonEmpty || bucketNumber <= 0

}
