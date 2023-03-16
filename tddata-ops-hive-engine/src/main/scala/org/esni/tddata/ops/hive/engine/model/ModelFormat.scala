package org.esni.tddata.ops.hive.engine.model

class ModelFormat(val value: String) {}

object ModelFormat {

  def apply(value: String): ModelFormat = new ModelFormat(value)

  val TEXT: ModelFormat = ModelFormat("text")

}
