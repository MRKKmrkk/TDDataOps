package org.esni.tddata.ops.hive.engine.exception

class PartitionColumnsIsEmptyError(msg: String) extends Error(msg) {}

object PartitionColumnsIsEmptyError{

  def apply(msg: String): PartitionColumnsIsEmptyError = new PartitionColumnsIsEmptyError(msg)

}
