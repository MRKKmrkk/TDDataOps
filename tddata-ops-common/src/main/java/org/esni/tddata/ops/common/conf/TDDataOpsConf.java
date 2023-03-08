package org.esni.tddata.ops.common.conf;

import java.io.*;
import java.util.Objects;
import java.util.Properties;

public class TDDataOpsConf {

    private static TDDataOpsConf TDDATA_OPS_CONF = null;

    /**
     * 创建测试模式的配置文件类
     * 如果配置文件类已经被创建，则只会返回之前创建的配置文件类
     */
    public static TDDataOpsConf getOrCreateTDDataOpsConfWithTestMode() throws IOException {

        if (TDDATA_OPS_CONF == null) {
            TDDATA_OPS_CONF = new TDDataOpsConf(true);
        }

        return TDDATA_OPS_CONF;

    }

    /**
     * 创建非测试模式的配置文件类
     * 如果配置文件类已经被创建，则只会返回之前创建的配置文件类
     */
    public static TDDataOpsConf getOrCreateTDDataOpsConf() throws IOException {

        if (TDDATA_OPS_CONF == null) {
            TDDATA_OPS_CONF = new TDDataOpsConf(false);
        }

        return TDDATA_OPS_CONF;

    }

    private final Properties properties;
    private final boolean isTestMode;

    /**
     * 如果为测试模式则从resources目录下获取配置文件
     * 如果不为测试模式则从 $TDDATA_OPS_HOME/config 下获取配置文件
     */
    private TDDataOpsConf(boolean isTestMode) throws IOException {

        this.properties = new Properties();
        this.isTestMode = isTestMode;
        InputStream stream;

        if (isTestMode) {

            stream = Thread
                    .currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream("tddata_ops.properties");

        } else {
            String homePath = System.getenv("TDDATA_OPS_HOME");
            stream = new FileInputStream(homePath + File.separator + "config" + File.separator + "tddata_ops.properties");
        }

        this.properties.load(stream);

        if (Objects.nonNull(stream)) {
            stream.close();
        }

    }

    public Object get(String key) {

        return properties.get(key);

    }

    public Object get(String key, Object defaultVal) {

        Object val = properties.get(key);
        return val == null ? defaultVal : val;

    }

    public String getString(String key) {

        return properties.getProperty(key);

    }

    public String getString(String key, String defaultVal) {

        String val = properties.getProperty(key);
        return val == null ? defaultVal : val;

    }

    public int getInt(String key) {

        return (int) get(key);

    }

    public int getInt(String key, int defaultVal) {

        return (int) get(key, defaultVal);

    }

    public long getLong(String key) {

        return (long) get(key);

    }

    public long getLong(String key, long defaultVal) {

        return (long) get(key, defaultVal);

    }

    public void setString(String key, String val) {

        properties.setProperty(key, val);

    }

    public void setInt(String key, int val) {

        properties.setProperty(key, String.valueOf(val));

    }

    public void setLong(String key, long val) {

        properties.setProperty(key, String.valueOf(val));

    }

    /**
     * 导出所有配置文件
     */
    public void export() throws IOException {

        String path;
        if (isTestMode) {
            path = Objects.requireNonNull(
                    Thread.currentThread()
                            .getContextClassLoader()
                            .getResource(""))
                    .getPath()
                    .replace("/target/classes/", "") + File.separator + "src" + File.separator + "main" + File.separator + "resources" + File.separator + "tddata_ops.properties";
        }
        else {
            String homePath = System.getenv("TDDATA_OPS_HOME");
            path = homePath + File.separator + "config" + File.separator + "tddata_ops.properties";
        }

        FileOutputStream outStream = new FileOutputStream(new File(path));
        properties.store(outStream, "");
        outStream.close();

    }

    public static void main(String[] args) throws IOException {

        TDDataOpsConf conf = TDDataOpsConf.getOrCreateTDDataOpsConfWithTestMode();
        System.out.println(conf.getString("aaaaqqqqghjiuhgilyisiopjiodjsiowdjssjiosjiou888888888888888888888888888888888yglghiluyqqqq", "default"));

    }

}
