package org.esni.tddata.ops.hive.engine.exception

class HDFSURIError(msg: String) extends Error(msg) {}

object HDFSURIError {

  def apply(msg: String): HDFSURIError = new HDFSURIError(msg)

}
