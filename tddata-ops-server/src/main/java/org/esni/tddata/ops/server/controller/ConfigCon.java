package org.esni.tddata.ops.server.controller;

import org.esni.tddata.ops.server.config.ReadConfigFile;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
@RequestMapping("/")
public class ConfigCon {

    @Resource
    ReadConfigFile readConfigFile;

    @RequestMapping("/test")
    public ReadConfigFile getReadConfigFile() {
        return readConfigFile;
    }
}
