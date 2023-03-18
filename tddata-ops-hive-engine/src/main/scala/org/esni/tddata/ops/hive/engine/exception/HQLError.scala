package org.esni.tddata.ops.hive.engine.exception

class HQLError(msg: String) extends Error(msg){

}

object HQLError {

  def apply(msg: String): HQLError = new HQLError(msg)

}
