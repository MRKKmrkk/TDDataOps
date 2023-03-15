package org.esni.tddata.ops.hive.engine.table

import org.apache.spark.sql.types.StructType
import org.esni.tddata.ops.hive.engine.exception.{HDFSURIError, PartitionColumnsIsEmptyError}

class HiveTable  (
                 var workspace: String,
                 var layerName: String,
                 var modelName: String,
                 var schema: StructType,
                 var format: FileFormat,
                 var path: String,
                 var isExternalTable: Boolean,
                 var partitionColumns: Array[String],
                 var isPartitionTable: Boolean
               ) {

}

class HiveTableBuilder(
                        private var workspace: String,
                        private var layerName: String,
                        private var modelName: String,
                        private var schema: StructType,
                        private var format: FileFormat
                      ) {

  private var path: String = _
  private var isExternalTable: Boolean = false
  private var partitionColumns: Array[String] = _
  private var isPartitionTable: Boolean = false

  def externalTable(path: String): HiveTableBuilder = {

    if (!path.startsWith("hdfs://")) throw HDFSURIError(f"uri must start with 'hdfs://' , but got '$path'")

    this.path = path
    this.isExternalTable = true

    this

  }

  def partition(column: String*): HiveTableBuilder = {

    this.partitionColumns = column.toArray
    this.isPartitionTable = true

    this

  }

  def partition(columns: Array[String]): HiveTableBuilder = {

    if (columns.isEmpty) throw PartitionColumnsIsEmptyError("partition columns can not be empty")

    this.partitionColumns = columns
    this.isPartitionTable = true

    this

  }

  def build(): HiveTable = {

    new HiveTable(workspace, layerName, modelName, schema, format, path, isExternalTable, partitionColumns, isPartitionTable)

  }

}

object HiveTableBuilder {

  def apply(workspace: String, layerName: String, modelName: String, schema: StructType, format: FileFormat): HiveTableBuilder = new HiveTableBuilder(workspace, layerName, modelName, schema, format)

}


