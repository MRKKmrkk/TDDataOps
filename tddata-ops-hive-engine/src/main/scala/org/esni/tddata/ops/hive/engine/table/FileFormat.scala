package org.esni.tddata.ops.hive.engine.table

class FileFormat(val value: String) {}

object FileFormat {

  def apply(value: String): FileFormat = new FileFormat(value)

  val TEXT: FileFormat = FileFormat("textfile")
  val PARQUET: FileFormat = FileFormat("parquet")

}
