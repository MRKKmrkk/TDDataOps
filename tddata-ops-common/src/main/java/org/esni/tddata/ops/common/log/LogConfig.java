package org.esni.tddata.ops.common.log;

import ch.qos.logback.core.PropertyDefinerBase;
import org.esni.tddata.ops.common.conf.TDDataOpsConf;

import java.io.File;
import java.io.IOException;

public class LogConfig extends PropertyDefinerBase {


  @Override
  public String getPropertyValue() {
    try {
      TDDataOpsConf conf = TDDataOpsConf.getOrCreateTDDataOpsConfWithTestMode();
      return conf.getString("server.log.dir", System.getenv("TDDATA_OPD_HOME") + File.separator + "log");
    } catch (IOException e) {
      return null;
    }
  }
}