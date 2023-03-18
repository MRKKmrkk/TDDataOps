package org.esni.tddata.ops.hive.engine.exception

class EmptyWorkSpaceError(msg: String) extends Error(msg) {

}

object EmptyWorkSpaceError {

  def apply(msg: String): EmptyWorkSpaceError = new EmptyWorkSpaceError(msg)

}
