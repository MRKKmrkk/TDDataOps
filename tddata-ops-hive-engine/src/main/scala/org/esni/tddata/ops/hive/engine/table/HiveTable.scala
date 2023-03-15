package org.esni.tddata.ops.hive.engine.table

import org.apache.spark.sql.types.StructType
import org.esni.tddata.ops.hive.engine.model.ModelFormat

class HiveTable private (
                 var workspace: String,
                 var layerName: String,
                 var modelName: String,
                 var schema: StructType,
                 var format: ModelFormat,
                 var path: String,
                 var isExternalTable: Boolean,
                 var partitionColumns: Array[String],
                 var isPartitionTable: Boolean
               ) {

  class HiveTableBuilder(
                          var workspace: String,
                          var layerName: String,
                          var modelName: String,
                          var schema: StructType,
                          var format: ModelFormat
                        ) {

    var path: String = _
    var isExternalTable: Boolean = false
    var partitionColumns: Array[String] = _
    var isPartitionTable: Boolean = false

    def externalTable(path: String): HiveTableBuilder = {

      this.path = path
      this.isExternalTable = true

      this

    }

    def partition(column: String): HiveTableBuilder = {

      this.partitionColumns = Array(column)
      this.isPartitionTable = true

      this

    }

    def partition(columns: Array[String]): HiveTableBuilder = {

      this.partitionColumns = columns
      this.isPartitionTable = true

      this

    }

    def build(): HiveTable = {

      HiveTable(workspace, layerName, modelName, schema, format, path, isExternalTable, partitionColumns, isPartitionTable)

    }

  }

  object HiveTableBuilder {

    def apply(workspace: String, layerName: String, modelName: String, schema: StructType, format: ModelFormat): HiveTableBuilder = new HiveTableBuilder(workspace, layerName, modelName, schema, format)

  }

}

object HiveTable {

  def apply(workspace: String, layerName: String, modelName: String, schema: StructType, format: ModelFormat, path: String, isExternalTable: Boolean, partitionColumns: Array[String], isPartitionTable: Boolean): HiveTable = new HiveTable(workspace, layerName, modelName, schema, format, path, isExternalTable, partitionColumns, isPartitionTable)

}
