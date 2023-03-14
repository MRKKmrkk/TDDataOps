package org.esni.tddata.ops.hive.engine.model
import org.apache.spark.sql.types.StructType

class MySQLModel(
                  var workspace: String,
                  var layerName: String,
                  var modelName: String
                ) extends Model {



}
