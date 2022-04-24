package com.xiaopeng.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 项目启动初始化的时候,自动加载配置文件参数
 */
public class QuotConfig {

    public static Properties config = new Properties();
    static {
        InputStream ins = QuotConfig.class.getClassLoader().getResourceAsStream("config.properties");

        try {
            config.load(ins);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println(config.getProperty("zookeeper.connect"));
    }
}
