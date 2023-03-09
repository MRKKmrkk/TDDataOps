package org.esni.tddata.ops.server.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.esni.tddata.ops.common.conf.TDDataOpsConf;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Service
@PropertySource(value = {"classpath:/tddata_ops.properties"})
public class ReadConfigFile {

    public String getTdDataOpsConf() throws IOException {

        String driver = TDDataOpsConf.getOrCreateTDDataOpsConfWithTestMode().getString("mysql.driver");
        String url = TDDataOpsConf.getOrCreateTDDataOpsConfWithTestMode().getString("mysql.url");
        String username = TDDataOpsConf.getOrCreateTDDataOpsConfWithTestMode().getString("mysql.username");
        String password = TDDataOpsConf.getOrCreateTDDataOpsConfWithTestMode().getString("mysql.password");
        String test = TDDataOpsConf.getOrCreateTDDataOpsConfWithTestMode().getString("demo","shabi");
        Map<String,Object> map = new HashMap<>();
        map.put("mysql.driver",driver);
        map.put("mysql.url",url);
        map.put("mysql.username",username);
        map.put("mysql.password",password);
        map.put("demo1",test);
        ObjectMapper mapper = new ObjectMapper();

        return mapper.writeValueAsString(map);
    }



}
