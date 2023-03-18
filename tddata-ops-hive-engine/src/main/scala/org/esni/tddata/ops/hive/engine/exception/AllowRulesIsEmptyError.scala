package org.esni.tddata.ops.hive.engine.exception

class AllowRulesIsEmptyError(msg: String) extends Error(msg) {

}

object AllowRulesIsEmptyError {

  def apply(msg: String): AllowRulesIsEmptyError = new AllowRulesIsEmptyError(msg)

}
